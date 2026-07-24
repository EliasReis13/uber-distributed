"""Servidor Uber distribuído — um mês por nó, agregação via /summary."""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.request import urlopen

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Cluster fixo (3 nós) ──────────────────────────────────────────────────────
CSV_BASE = (
    "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/"
    "master/uber-trip-data"
)

NODES = {
    8001: {
        "server_id": "servidor_01",
        "data_file": f"{CSV_BASE}/uber-raw-data-apr14.csv",
        "partition": "Abril/2014",
    },
    8002: {
        "server_id": "servidor_02",
        "data_file": f"{CSV_BASE}/uber-raw-data-may14.csv",
        "partition": "Maio/2014",
    },
    8003: {
        "server_id": "servidor_03",
        "data_file": f"{CSV_BASE}/uber-raw-data-jun14.csv",
        "partition": "Junho/2014",
    },
}

SERVER_PORT = int(os.getenv("SERVER_PORT", "8001"))
if SERVER_PORT not in NODES:
    raise SystemExit(f"SERVER_PORT inválida: {SERVER_PORT}. Use 8001, 8002 ou 8003.")

NODE = NODES[SERVER_PORT]
SERVER_ID = NODE["server_id"]
KNOWN_SERVERS = [f"http://localhost:{p}" for p in NODES if p != SERVER_PORT]
HTTP_TIMEOUT = 10.0
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

PICKUP_CSV_FMT = "%m/%d/%Y %H:%M:%S"
PICKUP_DISPLAY_FMT = "%d/%m/%Y %H:%M:%S"

app = FastAPI(title=f"Uber Distributed — {SERVER_ID}", version="2.0.0")
records: list[dict[str, Any]] = []


def load_data() -> None:
    global records
    records = []
    source = NODE["data_file"]
    logger.info("Carregando %s …", source)

    try:
        if source.startswith("http"):
            with urlopen(source, timeout=60) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            rows = list(csv.DictReader(io.StringIO(text)))
        else:
            with open(source, encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
    except Exception as exc:
        logger.error("Falha ao carregar dados: %s", exc)
        return

    for row in rows:
        try:
            dt = datetime.strptime(row["Date/Time"].strip(), PICKUP_CSV_FMT)
        except (KeyError, ValueError):
            continue
        records.append({
            "datetime": dt,
            "base": (row.get("Base") or "").strip(),
        })

    logger.info("%s: %d registros (%s)", SERVER_ID, len(records), NODE["partition"])


def parse_date(value: str, name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Formato inválido para '{name}'. Use YYYY-MM-DD.")


def summarize(start: date, end: date, base: Optional[str] = None) -> dict:
    count = 0
    base_counts: dict[str, int] = {}
    first_dt: Optional[datetime] = None
    last_dt: Optional[datetime] = None

    for r in records:
        d = r["datetime"].date()
        if d < start or d > end:
            continue
        if base and r["base"] != base:
            continue
        count += 1
        base_counts[r["base"]] = base_counts.get(r["base"], 0) + 1
        dt = r["datetime"]
        if first_dt is None or dt < first_dt:
            first_dt = dt
        if last_dt is None or dt > last_dt:
            last_dt = dt

    return {
        "pickup_count": count,
        "base_counts": base_counts,
        "first_pickup": first_dt.strftime(PICKUP_DISPLAY_FMT) if first_dt else None,
        "last_pickup": last_dt.strftime(PICKUP_DISPLAY_FMT) if last_dt else None,
    }


def merge_results(parts: list[dict]) -> dict:
    total = 0
    base_counts: dict[str, int] = {}
    first_dt: Optional[datetime] = None
    last_dt: Optional[datetime] = None

    for res in parts:
        total += res.get("pickup_count", 0)
        for b, c in res.get("base_counts", {}).items():
            base_counts[b] = base_counts.get(b, 0) + c
        for field, better in (("first_pickup", min), ("last_pickup", max)):
            val = res.get(field)
            if not val:
                continue
            try:
                dt = datetime.strptime(val, PICKUP_DISPLAY_FMT)
            except ValueError:
                continue
            if field == "first_pickup":
                first_dt = dt if first_dt is None else better(first_dt, dt)
            else:
                last_dt = dt if last_dt is None else better(last_dt, dt)

    return {
        "pickup_count": total,
        "base_counts": base_counts,
        "first_pickup": first_dt.strftime(PICKUP_DISPLAY_FMT) if first_dt else None,
        "last_pickup": last_dt.strftime(PICKUP_DISPLAY_FMT) if last_dt else None,
    }


@app.on_event("startup")
def startup() -> None:
    load_data()


@app.get("/", include_in_schema=False)
def home():
    path = STATIC_DIR / "index.html"
    if not path.exists():
        raise HTTPException(404, "static/index.html não encontrado")
    return FileResponse(path)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "server_id": SERVER_ID,
        "partition": NODE["partition"],
        "records": len(records),
    }


@app.get("/metadata")
def metadata():
    return {
        "server_id": SERVER_ID,
        "owns": {
            "partition_description": NODE["partition"],
        },
        "known_servers": KNOWN_SERVERS,
    }


@app.get("/cluster/status")
async def cluster_status():
    nodes: list[dict[str, Any]] = [
        {
            "url": f"http://localhost:{SERVER_PORT}",
            "server_id": SERVER_ID,
            "online": True,
            "self": True,
            "partition": NODE["partition"],
        }
    ]

    async def probe(server_url: str):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                health_resp = await client.get(f"{server_url.rstrip('/')}/health")
                health_resp.raise_for_status()
                health_data = health_resp.json()
                partition = health_data.get("partition")
                return {
                    "url": server_url,
                    "server_id": health_data.get("server_id", server_url),
                    "online": True,
                    "self": False,
                    "partition": partition,
                }
        except Exception:
            return {
                "url": server_url,
                "server_id": None,
                "online": False,
                "self": False,
                "partition": None,
            }

    nodes.extend(await asyncio.gather(*[probe(url) for url in KNOWN_SERVERS]))
    return {"coordinator": SERVER_ID, "nodes": nodes}


@app.get("/local/summary")
def local_summary(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    base: Optional[str] = Query(None),
):
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    return {
        "server_id": SERVER_ID,
        "scope": "local",
        "complete": True,
        "result": summarize(start, end, base),
    }


@app.get("/summary")
async def distributed_summary(
    start_date: str = Query(...),
    end_date: str = Query(...),
    base: Optional[str] = Query(None),
):
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if base:
        params["base"] = base

    local = summarize(start, end, base)
    contacted = [SERVER_ID]
    failed: list[str] = []
    parts = [local]

    async def fetch(url: str):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(f"{url.rstrip('/')}/local/summary", params=params)
                r.raise_for_status()
                data = r.json()
                return data.get("result"), data.get("server_id") or url, None
        except Exception as exc:
            return None, url, str(exc)

    for result, server_ref, err in await asyncio.gather(*[fetch(u) for u in KNOWN_SERVERS]):
        if err or result is None:
            # tenta nome amigável pela porta conhecida
            port = None
            try:
                port = int(server_ref.rstrip("/").rsplit(":", 1)[-1])
            except ValueError:
                pass
            name = NODES.get(port, {}).get("server_id", server_ref) if port else server_ref
            failed.append(name)
        else:
            contacted.append(server_ref)
            parts.append(result)

    return {
        "scope": "distributed",
        "coordinator": SERVER_ID,
        "complete": len(failed) == 0,
        "servers_contacted": contacted,
        "failed_servers": failed,
        "result": merge_results(parts),
    }
