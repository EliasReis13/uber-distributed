"""Testes de integração do cluster Uber (requer servidores em 8001–8006).

Pré-requisito
-------------
Com o cluster no ar (``python start.py``)::

    pytest tests/test_server.py -v

A URL base pode ser sobrescrita com a variável ``TEST_SERVER_URL``
(padrão: ``http://localhost:8001``).
"""

from __future__ import annotations

import os
from typing import Any

import httpx

BASE_URL: str = os.getenv("TEST_SERVER_URL", "http://localhost:8001")


def get(path: str, **params: Any) -> httpx.Response:
    """GET em ``BASE_URL + path`` com query params opcionais.

    Args:
        path: Caminho do endpoint (ex.: ``"/health"``).
        **params: Query string repassada ao cliente HTTP.

    Returns:
        Resposta HTTP do nó sob teste.
    """
    return httpx.get(f"{BASE_URL}{path}", params=params, timeout=30)


def test_health() -> None:
    """``GET /health`` responde 200 com status ok e ``server_id``."""
    r = get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "server_id" in data


def test_metadata_structure() -> None:
    """``GET /metadata`` retorna ``owns.date_start/date_end`` e vizinhos conhecidos."""
    r = get("/metadata")
    assert r.status_code == 200
    data = r.json()
    assert "server_id" in data
    owns = data["owns"]
    assert "date_start" in owns
    assert "date_end" in owns
    assert "partition_description" in owns
    assert isinstance(data["known_servers"], list)


def test_local_summary() -> None:
    """``GET /local/summary`` retorna escopo local e contagem inteira."""
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "local"
    assert data["complete"] is True
    assert isinstance(data["result"]["pickup_count"], int)


def test_local_summary_invalid_dates() -> None:
    """Intervalo invertido em ``/local/summary`` retorna HTTP 400."""
    r = get("/local/summary", start_date="2014-05-01", end_date="2014-04-01")
    assert r.status_code == 400


def test_local_summary_bad_format() -> None:
    """Data fora de ``YYYY-MM-DD`` em ``/local/summary`` retorna HTTP 400."""
    r = get("/local/summary", start_date="01-04-2014", end_date="2014-04-30")
    assert r.status_code == 400


def test_distributed_summary() -> None:
    """``GET /summary`` retorna escopo distribuído e lista de servidores."""
    r = get("/summary", start_date="2014-04-01", end_date="2014-06-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "distributed"
    assert "coordinator" in data
    assert "servers_contacted" in data
    assert "failed_servers" in data
    assert data["coordinator"] in data["servers_contacted"]


def test_distributed_summary_skips_irrelevant_servers() -> None:
    """Consulta restrita ao mês do coordenador não contata vizinhos de outros meses."""
    r = get("/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["servers_contacted"] == [data["coordinator"]]
    assert data["failed_servers"] == []


def test_distributed_gte_local() -> None:
    """Contagem distribuída é maior ou igual à contagem só local."""
    local = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30").json()
    dist = get("/summary", start_date="2014-04-01", end_date="2014-04-30").json()
    assert dist["result"]["pickup_count"] >= local["result"]["pickup_count"]


def test_distributed_summary_query_field() -> None:
    """``GET /summary`` ecoa os parâmetros recebidos em ``query``."""
    r = get("/summary", start_date="2014-04-01", end_date="2014-04-30", base="B02512")
    assert r.status_code == 200
    query = r.json()["query"]
    assert query["start_date"] == "2014-04-01"
    assert query["end_date"] == "2014-04-30"
    assert query["base"] == "B02512"
