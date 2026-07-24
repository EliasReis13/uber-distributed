"""
Testes do cluster Uber (requer servidores em 8001–8003).

  pytest tests/test_server.py -v
"""

import os

import httpx

BASE_URL = os.getenv("TEST_SERVER_URL", "http://localhost:8001")


def get(path: str, **params):
    return httpx.get(f"{BASE_URL}{path}", params=params, timeout=30)


def test_health():
    r = get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "server_id" in data


def test_local_summary():
    r = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "local"
    assert data["complete"] is True
    assert isinstance(data["result"]["pickup_count"], int)


def test_local_summary_invalid_dates():
    r = get("/local/summary", start_date="2014-05-01", end_date="2014-04-01")
    assert r.status_code == 400


def test_local_summary_bad_format():
    r = get("/local/summary", start_date="01-04-2014", end_date="2014-04-30")
    assert r.status_code == 400


def test_distributed_summary():
    r = get("/summary", start_date="2014-04-01", end_date="2014-06-30")
    assert r.status_code == 200
    data = r.json()
    assert data["scope"] == "distributed"
    assert "coordinator" in data
    assert "servers_contacted" in data
    assert "failed_servers" in data
    assert data["coordinator"] in data["servers_contacted"]


def test_distributed_gte_local():
    local = get("/local/summary", start_date="2014-04-01", end_date="2014-04-30").json()
    dist = get("/summary", start_date="2014-04-01", end_date="2014-04-30").json()
    assert dist["result"]["pickup_count"] >= local["result"]["pickup_count"]
