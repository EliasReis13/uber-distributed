"""
Testes automatizados para o servidor Uber distribuído.
Execute com:  pytest tests/test_server.py -v
Requer que o servidor_01 esteja rodando em localhost:8001
e pelo menos servidor_02 em localhost:8002 para testes distribuídos.
"""

import pytest
import httpx
import os

BASE_URL = os.getenv("TEST_SERVER_URL", "http://localhost:8001")


# ── Helpers ───────────────────────────────────────────────────────────────────

def get(path: str, **params):
    url = f"{BASE_URL}{path}"
    return httpx.get(url, params=params, timeout=15)


# ── /health ───────────────────────────────────────────────────────────────────

def test_health_returns_ok():
    r = get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "server_id" in data


# ── /metadata ─────────────────────────────────────────────────────────────────

def test_metadata_structure():
    r = get("/metadata")
    assert r.status_code == 200
    data = r.json()
    assert "server_id" in data
    assert "owns" in data
    assert "known_servers" in data
    owns = data["owns"]
    assert "date_start" in owns
    assert "date_end" in owns


# ── /local/summary — parâmetros inválidos ─────────────────────────────────────

def test_local_summary_missing_start_date():
    r = get("/local/summary", end_date="2014-04-30")
    assert r.status_code == 422  # FastAPI validation


def test_local_summary_missing_end_date():
    r = get("/local/summary", start_date="2014-04-01")
    assert r.status_code == 422


def test_local_summary_invalid_date_format():
    r = get("/local/summary", start_date="01-04-2014", end_date="2014-04-30")
    assert r.status_code == 400


def test_local_summary_start_after_end():
    r = get("/local/summary", start_date="2014-05-01", end_date="2014-04-01")
    assert r.status_code == 400


# ── /local/summary — consultas válidas ───────────────────────────────────────

def test_local_summary_valid_range():
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "local"
    assert data["complete"] is True
    result = data["result"]
    assert isinstance(result["pickup_count"], int)
    assert result["pickup_count"] >= 0
    assert isinstance(result["base_counts"], dict)


def test_local_summary_empty_range():
    """Intervalo fora dos dados do servidor deve retornar 0 pickups."""
    r = get("/local/summary", start_date="2013-01-01", end_date="2013-01-31")
    assert r.status_code == 200
    data = r.json()
    assert data["result"]["pickup_count"] == 0
    assert data["result"]["first_pickup"] is None
    assert data["result"]["last_pickup"] is None


def test_local_summary_with_base_filter():
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30", base="B02512")
    assert r.status_code == 200
    data = r.json()
    result = data["result"]
    # Se há dados, todos os base_counts devem ser apenas B02512
    if result["pickup_count"] > 0:
        assert set(result["base_counts"].keys()) == {"B02512"}


def test_local_summary_base_counts_sum():
    """A soma dos base_counts deve igualar pickup_count."""
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    result = r.json()["result"]
    assert sum(result["base_counts"].values()) == result["pickup_count"]


def test_local_summary_first_last_pickup_order():
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    result = r.json()["result"]
    if result["first_pickup"] and result["last_pickup"]:
        from datetime import datetime
        fmt = "%d/%m/%Y %H:%M:%S"
        first = datetime.strptime(result["first_pickup"], fmt)
        last  = datetime.strptime(result["last_pickup"],  fmt)
        assert first <= last


# ── /summary (distribuída) ────────────────────────────────────────────────────

def test_distributed_summary_structure():
    r = get("/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "distributed"
    assert "coordinator" in data
    assert "servers_contacted" in data
    assert "failed_servers" in data
    assert "result" in data
    assert "trace_id" in data


def test_distributed_summary_coordinator_listed():
    r = get("/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["coordinator"] in data["servers_contacted"]


def test_distributed_summary_pickup_count_gte_local():
    """O total distribuído deve ser >= ao local."""
    local = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30").json()
    dist  = get("/summary",       start_date="2014-04-01", end_date="2014-04-30").json()
    assert dist["result"]["pickup_count"] >= local["result"]["pickup_count"]


def test_distributed_summary_missing_params():
    r = get("/summary", end_date="2014-04-30")
    assert r.status_code == 422


# ── /top-bases ────────────────────────────────────────────────────────────────

def test_top_bases():
    r = get("/top-bases", start_date="2014-04-01", end_date="2014-04-30", n=3)
    assert r.status_code == 200
    data = r.json()
    assert "top_bases" in data
    assert isinstance(data["top_bases"], list)
    assert len(data["top_bases"]) <= 3
    for item in data["top_bases"]:
        assert "base" in item
        assert "pickup_count" in item


def test_top_bases_sorted_desc():
    r = get("/top-bases", start_date="2014-04-01", end_date="2014-04-30", n=10)
    assert r.status_code == 200
    counts = [item["pickup_count"] for item in r.json()["top_bases"]]
    assert counts == sorted(counts, reverse=True)
