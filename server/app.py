import os
import csv
import uuid
import logging
import asyncio
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List, Any

import pandas as pd

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration (via env vars or defaults) ──────────────────────────────────
SERVER_ID    = os.getenv("SERVER_ID", "servidor_01")
SERVER_PORT  = int(os.getenv("SERVER_PORT", "8001"))
DATA_FILES   = os.getenv("DATA_FILES", "")          # comma-separated CSV paths
KNOWN_SERVERS = [
    s.strip()
    for s in os.getenv("KNOWN_SERVERS", "").split(",")
    if s.strip()
]
DATE_START   = os.getenv("DATE_START", "")
DATE_END     = os.getenv("DATE_END", "")
PARTITION_DESC = os.getenv("PARTITION_DESC", "Partição de dados Uber")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(
    title=f"Servidor Distribuído Uber — {SERVER_ID}",
    version="1.0.0",
    description="API REST para consultas distribuídas de dados de viagens Uber/NYC",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Data loading ──────────────────────────────────────────────────────────────
PICKUP_CSV_FMT = "%m/%d/%Y %H:%M:%S"       # formato do CSV Uber (mês/dia/ano)
PICKUP_DISPLAY_FMT = "%d/%m/%Y %H:%M:%S"   # exibição (dia/mês/ano)
records: List[Dict[str, Any]] = []


def parse_datetime(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value.strip(), PICKUP_CSV_FMT)
    except ValueError:
        return None


def load_data() -> None:
    global records
    records = []
    sources = [f.strip() for f in DATA_FILES.split(",") if f.strip()]
    if not sources:
        logger.warning("DATA_FILES não configurado — sem dados locais.")
        return

    for source in sources:
        logger.info("Carregando dados de: %s", source)
        try:
            df = pd.read_csv(source)
        except Exception as exc:
            logger.error("Falha ao carregar %s: %s", source, exc)
            continue

        count = 0
        for _, row in df.iterrows():
            dt = parse_datetime(str(row.get("Date/Time", "")))
            if dt is None:
                continue
            try:
                lat = float(row.get("Lat", 0))
                lon = float(row.get("Lon", 0))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0
            records.append({
                "datetime": dt,
                "lat": lat,
                "lon": lon,
                "base": str(row.get("Base", "")).strip(),
            })
            count += 1
        logger.info("Carregados %d registros de %s", count, source)

    logger.info("Total de registros locais: %d", len(records))


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_date_param(value: str, param_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Formato inválido para '{param_name}'. Use YYYY-MM-DD.",
        )


def filter_records(
    start: date,
    end: date,
    base: Optional[str] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
) -> List[Dict]:
    filtered = []
    for r in records:
        r_date = r["datetime"].date()
        if r_date < start or r_date > end:
            continue
        if base and r["base"] != base:
            continue
        if lat_min is not None and r["lat"] < lat_min:
            continue
        if lat_max is not None and r["lat"] > lat_max:
            continue
        if lon_min is not None and r["lon"] < lon_min:
            continue
        if lon_max is not None and r["lon"] > lon_max:
            continue
        filtered.append(r)
    return filtered


def build_result(filtered: List[Dict]) -> Dict:
    if not filtered:
        return {
            "pickup_count": 0,
            "base_counts": {},
            "first_pickup": None,
            "last_pickup": None,
        }

    base_counts: Dict[str, int] = {}
    first_dt = None
    last_dt = None

    for r in filtered:
        base_counts[r["base"]] = base_counts.get(r["base"], 0) + 1
        dt = r["datetime"]
        if first_dt is None or dt < first_dt:
            first_dt = dt
        if last_dt is None or dt > last_dt:
            last_dt = dt

    return {
        "pickup_count": len(filtered),
        "base_counts": base_counts,
        "first_pickup": first_dt.strftime(PICKUP_DISPLAY_FMT) if first_dt else None,
        "last_pickup": last_dt.strftime(PICKUP_DISPLAY_FMT) if last_dt else None,
    }


def merge_results(results: List[Dict]) -> Dict:
    """Agrega resultados parciais de múltiplos servidores."""
    if not results:
        return {"pickup_count": 0, "base_counts": {}, "first_pickup": None, "last_pickup": None}

    total_count = 0
    base_counts: Dict[str, int] = {}
    first_pickup: Optional[datetime] = None
    last_pickup: Optional[datetime] = None

    for res in results:
        total_count += res.get("pickup_count", 0)
        for base, cnt in res.get("base_counts", {}).items():
            base_counts[base] = base_counts.get(base, 0) + cnt
        for field, compare in [("first_pickup", lambda a, b: a < b), ("last_pickup", lambda a, b: a > b)]:
            val = res.get(field)
            if val:
                try:
                    dt = datetime.strptime(val, PICKUP_DISPLAY_FMT)
                except ValueError:
                    continue
                if field == "first_pickup":
                    if first_pickup is None or dt < first_pickup:
                        first_pickup = dt
                else:
                    if last_pickup is None or dt > last_pickup:
                        last_pickup = dt

    return {
        "pickup_count": total_count,
        "base_counts": base_counts,
        "first_pickup": first_pickup.strftime(PICKUP_DISPLAY_FMT) if first_pickup else None,
        "last_pickup": last_pickup.strftime(PICKUP_DISPLAY_FMT) if last_pickup else None,
    }


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup_event():
    load_data()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def ui_home():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(404, "Interface não encontrada (static/index.html).")
    return FileResponse(index)


@app.get("/health", summary="Verifica saúde do servidor")
def health():
    return {"status": "ok", "server_id": SERVER_ID}


@app.get("/cluster/status", summary="Status deste nó e dos servidores conhecidos")
async def cluster_status():
    nodes: List[Dict[str, Any]] = [
        {
            "url": f"http://localhost:{SERVER_PORT}",
            "server_id": SERVER_ID,
            "online": True,
            "self": True,
            "partition": PARTITION_DESC,
        }
    ]

    async def probe(server_url: str):
        try:
            async with httpx.AsyncClient(timeout=min(HTTP_TIMEOUT, 3.0)) as client:
                health_resp = await client.get(f"{server_url.rstrip('/')}/health")
                health_resp.raise_for_status()
                health_data = health_resp.json()
                partition = None
                try:
                    meta_resp = await client.get(f"{server_url.rstrip('/')}/metadata")
                    if meta_resp.is_success:
                        owns = meta_resp.json().get("owns") or {}
                        partition = owns.get("partition_description")
                except Exception:
                    pass
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

    remote = await asyncio.gather(*[probe(url) for url in KNOWN_SERVERS])
    nodes.extend(remote)
    return {"coordinator": SERVER_ID, "nodes": nodes}


@app.get("/metadata", summary="Metadados e servidores conhecidos")
def metadata():
    return {
        "server_id": SERVER_ID,
        "owns": {
            "date_start": DATE_START,
            "date_end": DATE_END,
            "partition_description": PARTITION_DESC,
        },
        "known_servers": KNOWN_SERVERS,
    }


@app.get("/local/summary", summary="Consulta local (sem chamar outros servidores)")
def local_summary(
    start_date: str = Query(..., description="Data inicial YYYY-MM-DD"),
    end_date:   str = Query(..., description="Data final YYYY-MM-DD"),
    base:       Optional[str]   = Query(None),
    lat_min:    Optional[float] = Query(None),
    lat_max:    Optional[float] = Query(None),
    lon_min:    Optional[float] = Query(None),
    lon_max:    Optional[float] = Query(None),
):
    start = parse_date_param(start_date, "start_date")
    end   = parse_date_param(end_date,   "end_date")

    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    filtered = filter_records(start, end, base, lat_min, lat_max, lon_min, lon_max)
    result   = build_result(filtered)

    return {
        "server_id": SERVER_ID,
        "scope": "local",
        "complete": True,
        "result": result,
    }


@app.get("/summary", summary="Consulta distribuída (coordenador)")
async def distributed_summary(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    base:       Optional[str]   = Query(None),
    lat_min:    Optional[float] = Query(None),
    lat_max:    Optional[float] = Query(None),
    lon_min:    Optional[float] = Query(None),
    lon_max:    Optional[float] = Query(None),
):
    trace_id = str(uuid.uuid4())[:8]
    logger.info("[%s] Consulta distribuída recebida por %s", trace_id, SERVER_ID)

    start = parse_date_param(start_date, "start_date")
    end   = parse_date_param(end_date,   "end_date")

    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    # 1) resultado local
    filtered      = filter_records(start, end, base, lat_min, lat_max, lon_min, lon_max)
    local_result  = build_result(filtered)

    servers_contacted = [SERVER_ID]
    failed_servers    = []
    all_results       = [local_result]

    # 2) consultar /local/summary de cada servidor remoto
    params: Dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if base:    params["base"]    = base
    if lat_min is not None: params["lat_min"] = lat_min
    if lat_max is not None: params["lat_max"] = lat_max
    if lon_min is not None: params["lon_min"] = lon_min
    if lon_max is not None: params["lon_max"] = lon_max

    async def fetch_remote(server_url: str):
        url = f"{server_url.rstrip('/')}/local/summary"
        logger.info("[%s] Chamando %s", trace_id, url)
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                return data.get("result"), server_url, None
        except Exception as exc:
            logger.error("[%s] Falha em %s: %s", trace_id, server_url, exc)
            return None, server_url, str(exc)

    tasks = [fetch_remote(s) for s in KNOWN_SERVERS]
    responses = await asyncio.gather(*tasks)

    for result_data, srv, error in responses:
        srv_id = srv
        if error or result_data is None:
            failed_servers.append(srv_id)
        else:
            servers_contacted.append(srv_id)
            all_results.append(result_data)

    merged   = merge_results(all_results)
    complete = len(failed_servers) == 0

    return {
        "trace_id": trace_id,
        "scope": "distributed",
        "coordinator": SERVER_ID,
        "query": {
            "start_date": start_date,
            "end_date": end_date,
            "base": base,
        },
        "complete": complete,
        "servers_contacted": servers_contacted,
        "failed_servers": failed_servers,
        "result": merged,
    }


@app.get("/top-bases", summary="Top N bases com mais pickups (local)")
def top_bases(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    n:          int = Query(5, ge=1, le=50),
):
    start = parse_date_param(start_date, "start_date")
    end   = parse_date_param(end_date,   "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    filtered = filter_records(start, end)
    result   = build_result(filtered)

    sorted_bases = sorted(
        result["base_counts"].items(), key=lambda x: x[1], reverse=True
    )[:n]

    return {
        "server_id": SERVER_ID,
        "scope": "local",
        "top_bases": [{"base": b, "pickup_count": c} for b, c in sorted_bases],
    }
