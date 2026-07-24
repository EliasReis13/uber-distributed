"""Servidor FastAPI de um nó do cluster Uber NYC 2014.

Cada processo atende uma porta (``SERVER_PORT``: 8001 a 8006) e carrega
apenas a partição mensal correspondente (abril a setembro/2014) em um banco
SQLite em disco (``data/<server_id>.db``), indexado por data.

``SERVER_PORT``, ``KNOWN_SERVERS`` e ``HTTP_TIMEOUT`` podem ser definidos
por variáveis de ambiente ou por um arquivo ``.env`` na raiz do projeto
(veja ``.env.example``) — útil quando cada máquina roda sempre o mesmo nó.

Na primeira subida o CSV é importado; nas seguintes o ``.db`` já existente
é reaproveitado (startup bem mais rápido).

Endpoints principais:

* ``GET /local/summary`` — agrega só os registros deste nó.
* ``GET /summary`` — coordena a consulta chamando ``/local/summary`` nos
  vizinhos e mescla os resultados (sem ciclo de coordenação).
* ``GET /local/insights`` / ``GET /insights`` — cruzamentos de hora do dia,
  dia da semana e zona geográfica (Lat/Lon) para apoiar decisões de negócio.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional, TypedDict
from urllib.request import urlopen

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse

# Carrega variáveis de um arquivo ``.env`` na raiz do projeto (se existir),
# permitindo configurar SERVER_PORT/KNOWN_SERVERS/HTTP_TIMEOUT sem exportar
# nada manualmente no shell a cada terminal aberto. Não sobrescreve
# variáveis já definidas no ambiente (override=False por padrão), então
# `$env:KNOWN_SERVERS = "..."` continua tendo prioridade sobre o `.env`.
# Garante que funcione também quando o módulo é executado diretamente via
# `uvicorn server.app:app` fora da raiz do projeto.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Tipos ─────────────────────────────────────────────────────────────────────

class NodeConfig(TypedDict):
    """Configuração estática de um nó do cluster."""

    server_id: str
    data_file: str
    partition: str
    date_start: str
    date_end: str


class SummaryResult(TypedDict):
    """Resumo numérico de embarques em um intervalo de datas."""

    pickup_count: int
    base_counts: dict[str, int]
    first_pickup: Optional[str]
    last_pickup: Optional[str]


class ZoneCount(TypedDict):
    """Uma célula de grade geográfica (~1,1 km) e sua contagem de pickups."""

    lat: float
    lon: float
    count: int


class LocalInsightsResult(TypedDict):
    """Cruzamento de hora do dia, dia da semana e zona geográfica."""

    by_hour: dict[str, int]
    by_weekday: dict[str, int]
    top_zones: list[ZoneCount]


class HealthResponse(TypedDict):
    """Resposta de ``GET /health``."""

    status: str
    server_id: str
    partition: str
    records: int


class MetadataOwns(TypedDict):
    """Campo ``owns`` da resposta de ``GET /metadata``."""

    date_start: str
    date_end: str
    partition_description: str


class MetadataResponse(TypedDict):
    """Resposta de ``GET /metadata``."""

    server_id: str
    owns: MetadataOwns
    known_servers: list[str]


class ClusterNodeStatus(TypedDict):
    """Status de um nó reportado por ``GET /cluster/status``."""

    url: str
    server_id: Optional[str]
    online: bool
    self: bool
    partition: Optional[str]


class ClusterStatusResponse(TypedDict):
    """Resposta de ``GET /cluster/status``."""

    coordinator: str
    nodes: list[ClusterNodeStatus]


class LocalSummaryResponse(TypedDict):
    """Resposta de ``GET /local/summary``."""

    server_id: str
    scope: str
    complete: bool
    result: SummaryResult


class SummaryQuery(TypedDict):
    """Campo ``query`` ecoado na resposta de ``GET /summary``."""

    start_date: str
    end_date: str
    base: Optional[str]


class DistributedSummaryResponse(TypedDict):
    """Resposta de ``GET /summary`` (consulta coordenada)."""

    scope: str
    coordinator: str
    query: SummaryQuery
    complete: bool
    servers_contacted: list[str]
    failed_servers: list[str]
    result: SummaryResult


class LocalInsightsResponse(TypedDict):
    """Resposta de ``GET /local/insights``."""

    server_id: str
    scope: str
    complete: bool
    result: LocalInsightsResult


class InsightsResponse(TypedDict):
    """Resposta de ``GET /insights`` (versão distribuída)."""

    scope: str
    coordinator: str
    query: SummaryQuery
    complete: bool
    servers_contacted: list[str]
    failed_servers: list[str]
    result: LocalInsightsResult


# ── Cluster fixo (6 nós, abril a setembro/2014) ───────────────────────────────
CSV_BASE = (
    "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/"
    "master/uber-trip-data"
)
NODES: dict[int, NodeConfig] = {
    8001: {
        "server_id": "servidor_01",
        "data_file": f"{CSV_BASE}/uber-raw-data-apr14.csv",
        "partition": "Abril/2014",
        "date_start": "2014-04-01",
        "date_end": "2014-04-30",
    },
    8002: {
        "server_id": "servidor_02",
        "data_file": f"{CSV_BASE}/uber-raw-data-may14.csv",
        "partition": "Maio/2014",
        "date_start": "2014-05-01",
        "date_end": "2014-05-31",
    },
    8003: {
        "server_id": "servidor_03",
        "data_file": f"{CSV_BASE}/uber-raw-data-jun14.csv",
        "partition": "Junho/2014",
        "date_start": "2014-06-01",
        "date_end": "2014-06-30",
    },
    8004: {
        "server_id": "servidor_04",
        "data_file": f"{CSV_BASE}/uber-raw-data-jul14.csv",
        "partition": "Julho/2014",
        "date_start": "2014-07-01",
        "date_end": "2014-07-31",
    },
    8005: {
        "server_id": "servidor_05",
        "data_file": f"{CSV_BASE}/uber-raw-data-aug14.csv",
        "partition": "Agosto/2014",
        "date_start": "2014-08-01",
        "date_end": "2014-08-31",
    },
    8006: {
        "server_id": "servidor_06",
        "data_file": f"{CSV_BASE}/uber-raw-data-sep14.csv",
        "partition": "Setembro/2014",
        "date_start": "2014-09-01",
        "date_end": "2014-09-30",
    },
}

SERVER_PORT: int = int(os.getenv("SERVER_PORT", "8001"))
if SERVER_PORT not in NODES:
    _valid_ports = ", ".join(str(p) for p in sorted(NODES))
    raise SystemExit(f"SERVER_PORT inválida: {SERVER_PORT}. Use uma destas: {_valid_ports}.")

NODE: NodeConfig = NODES[SERVER_PORT]
SERVER_ID: str = NODE["server_id"]

# Nomes das bases TLC presentes nos CSVs abr–set/2014 (FiveThirtyEight).
# B02765/B02835/B02836 existem no README do FOIL, mas só nos dados de 2015.
BASE_NAMES: dict[str, str] = {
    "B02512": "Unter",
    "B02598": "Hinter",
    "B02617": "Weiter",
    "B02682": "Schmecken",
    "B02764": "Danach-NY",
}
_BASE_BY_NAME: dict[str, str] = {
    name.casefold(): code for code, name in BASE_NAMES.items()
}

# KNOWN_SERVERS pode ser configurado via variável de ambiente para apontar
# para máquinas reais na rede, ex.:
#   KNOWN_SERVERS=http://192.168.1.12:8002,http://192.168.1.13:8003
# Se não informado, assume os outros nós na mesma máquina (localhost) —
# útil para testar o cluster inteiro num só computador.
_known_servers_env: str = os.getenv("KNOWN_SERVERS", "").strip()
if _known_servers_env:
    KNOWN_SERVERS: list[str] = [
        u.strip().rstrip("/") for u in _known_servers_env.split(",") if u.strip()
    ]
else:
    KNOWN_SERVERS = [f"http://localhost:{p}" for p in NODES if p != SERVER_PORT]

HTTP_TIMEOUT: float = float(os.getenv("HTTP_TIMEOUT", "10"))
ROOT_DIR: Path = Path(__file__).resolve().parent.parent
STATIC_DIR: Path = ROOT_DIR / "static"
DATA_DIR: Path = ROOT_DIR / "data"
DB_PATH: Path = DATA_DIR / f"{SERVER_ID}.db"

PICKUP_CSV_FMT: str = "%m/%d/%Y %H:%M:%S"
PICKUP_SQL_FMT: str = "%Y-%m-%d %H:%M:%S"  # formato de exibição do PDF (e chave de ordenação em texto)
GEO_GRID_PRECISION: int = 2  # ~1,1 km de lado na latitude de NYC
TOP_ZONES_LIMIT: int = 12

app = FastAPI(title=f"Uber Distributed — {SERVER_ID}", version="3.1.0")

# ── Armazenamento: SQLite em disco, indexado por data ─────────────────────────
# Uma única conexão compartilhada entre as threads do FastAPI (endpoints
# síncronos rodam no threadpool do Starlette); `_db_lock` serializa o acesso,
# suficiente para o volume de consultas deste projeto.
DATA_DIR.mkdir(parents=True, exist_ok=True)
_conn: sqlite3.Connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
_db_lock = threading.Lock()
RECORD_COUNT: int = 0


_REQUIRED_PICKUP_COLUMNS: frozenset[str] = frozenset({"pickup_dt", "pickup_date", "base", "lat", "lon"})


def _table_ready() -> bool:
    """Retorna True se ``pickups`` existe, tem o schema atual (com ``lat``/``lon``) e ao menos 1 linha.

    Um ``.db`` gerado antes da coluna ``lat``/``lon`` existir é detectado
    como schema antigo (colunas faltando) e força reimportação automática
    em :func:`load_data`, sem precisar apagar o arquivo manualmente.
    """
    try:
        cols = {row[1] for row in _conn.execute("PRAGMA table_info(pickups)")}
        if not _REQUIRED_PICKUP_COLUMNS <= cols:
            return False
        count = _conn.execute("SELECT COUNT(*) FROM pickups").fetchone()[0]
        return count > 0
    except sqlite3.Error:
        return False


def _parse_float(value: Optional[str]) -> Optional[float]:
    """Converte uma string em ``float``, retornando ``None`` se vazia/inválida.

    Usado para ``Lat``/``Lon`` do CSV: preferimos manter a linha (com
    coordenada nula) a descartá-la só por falta/erro de geolocalização.
    """
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _import_csv() -> None:
    """Baixa/lê o CSV da partição e popula a tabela ``pickups`` no ``.db``."""
    source = NODE["data_file"]
    logger.info("Importando CSV em %s a partir de %s …", DB_PATH.name, source)

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
        rows = []

    def parsed_rows():
        for row in rows:
            try:
                dt = datetime.strptime(row["Date/Time"].strip(), PICKUP_CSV_FMT)
            except (KeyError, ValueError):
                continue
            base = (row.get("Base") or "").strip()
            dt_str = dt.strftime(PICKUP_SQL_FMT)
            lat = _parse_float(row.get("Lat"))
            lon = _parse_float(row.get("Lon"))
            yield dt_str, dt_str[:10], base, lat, lon

    _conn.execute("DROP TABLE IF EXISTS pickups")
    _conn.execute(
        """
        CREATE TABLE pickups (
            pickup_dt   TEXT NOT NULL,
            pickup_date TEXT NOT NULL,
            base        TEXT NOT NULL,
            lat         REAL,
            lon         REAL
        )
        """
    )
    _conn.executemany("INSERT INTO pickups VALUES (?, ?, ?, ?, ?)", parsed_rows())
    _conn.execute("CREATE INDEX idx_pickup_date ON pickups(pickup_date)")
    _conn.execute("CREATE INDEX idx_pickup_date_base ON pickups(pickup_date, base)")
    _conn.execute("CREATE INDEX idx_pickup_date_geo ON pickups(pickup_date, lat, lon)")
    _conn.commit()


def load_data() -> None:
    """Abre o SQLite em disco; importa o CSV só se o banco ainda estiver vazio.

    Aceita URL HTTP (download) ou caminho local em ``NODE["data_file"]``.
    Linhas com data inválida ou coluna ausente são ignoradas. Em caso de
    falha de I/O, registra o erro e deixa a tabela vazia.
    """
    global RECORD_COUNT

    with _db_lock:
        if _table_ready():
            RECORD_COUNT = _conn.execute("SELECT COUNT(*) FROM pickups").fetchone()[0]
            logger.info(
                "%s: reaproveitando %s (%d registros, %s)",
                SERVER_ID, DB_PATH.name, RECORD_COUNT, NODE["partition"],
            )
            return

        _import_csv()
        RECORD_COUNT = _conn.execute("SELECT COUNT(*) FROM pickups").fetchone()[0]

    logger.info(
        "%s: %d registros em %s (%s)",
        SERVER_ID, RECORD_COUNT, DB_PATH.name, NODE["partition"],
    )


def parse_date(value: str, name: str) -> date:
    """Converte string ``YYYY-MM-DD`` em :class:`~datetime.date`.

    Args:
        value: Data no formato ISO ``YYYY-MM-DD``.
        name: Nome do parâmetro (usado na mensagem de erro).

    Returns:
        Objeto :class:`~datetime.date` correspondente.

    Raises:
        HTTPException: Se o formato for inválido (status 400).
    """
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Formato inválido para '{name}'. Use YYYY-MM-DD.")


def resolve_base_filter(base: Optional[str]) -> Optional[str]:
    """Normaliza o filtro ``base`` para o código TLC canônico.

    Aceita código (``B02682``), nome (``Schmecken``) ou ``Nome (código)``.
    Comparação de nome é case-insensitive. Valores desconhecidos são
    devolvidos como vieram (a consulta tende a retornar zero).
    """
    if not base:
        return None
    raw = base.strip()
    if not raw:
        return None
    if raw.endswith(")") and "(" in raw:
        maybe_code = raw[raw.rfind("(") + 1 : -1].strip().upper()
        if maybe_code in BASE_NAMES:
            return maybe_code
    upper = raw.upper()
    if upper in BASE_NAMES:
        return upper
    by_name = _BASE_BY_NAME.get(raw.casefold())
    if by_name:
        return by_name
    return raw


def summarize(start: date, end: date, base: Optional[str] = None) -> SummaryResult:
    """Conta embarques locais no intervalo ``[start, end]`` via SQL indexado.

    Args:
        start: Data inicial (inclusiva).
        end: Data final (inclusiva).
        base: Se informado, filtra pela coluna Base do CSV (código TLC).

    Returns:
        Contagem total, contagens por base e timestamps do primeiro/último
        embarque no formato ``YYYY-MM-DD HH:MM:SS`` (igual ao especificado
        no enunciado do trabalho).
    """
    where = "pickup_date >= ? AND pickup_date <= ?"
    args: list[Any] = [start.isoformat(), end.isoformat()]
    if base:
        where += " AND base = ?"
        args.append(base)

    with _db_lock:
        rows = _conn.execute(
            f"SELECT base, COUNT(*) FROM pickups WHERE {where} GROUP BY base", args
        ).fetchall()
        first_raw, last_raw = _conn.execute(
            f"SELECT MIN(pickup_dt), MAX(pickup_dt) FROM pickups WHERE {where}", args
        ).fetchone()

    base_counts = {b: c for b, c in rows}
    return {
        "pickup_count": sum(base_counts.values()),
        "base_counts": base_counts,
        "first_pickup": first_raw,
        "last_pickup": last_raw,
    }


def merge_results(parts: list[SummaryResult]) -> SummaryResult:
    """Mescla vários :class:`SummaryResult` em um único resumo agregado.

    Soma ``pickup_count`` e ``base_counts``; escolhe o menor ``first_pickup``
    e o maior ``last_pickup`` entre as partes (comparação em texto funciona
    porque as datas estão no formato ordenável ``YYYY-MM-DD HH:MM:SS``).

    Args:
        parts: Lista de resumos parciais (local + vizinhos).

    Returns:
        Resumo consolidado com os mesmos campos de :class:`SummaryResult`.
    """
    total = 0
    base_counts: dict[str, int] = {}
    first_dt: Optional[str] = None
    last_dt: Optional[str] = None

    for res in parts:
        total += res.get("pickup_count", 0)
        for b, c in res.get("base_counts", {}).items():
            base_counts[b] = base_counts.get(b, 0) + c
        first_val = res.get("first_pickup")
        if first_val and (first_dt is None or first_val < first_dt):
            first_dt = first_val
        last_val = res.get("last_pickup")
        if last_val and (last_dt is None or last_val > last_dt):
            last_dt = last_val

    return {
        "pickup_count": total,
        "base_counts": base_counts,
        "first_pickup": first_dt,
        "last_pickup": last_dt,
    }


def insights(start: date, end: date, base: Optional[str] = None) -> LocalInsightsResult:
    """Cruza hora do dia, dia da semana e zona geográfica no intervalo local.

    Usa SQL indexado (sem carregar linhas em Python). Zonas são células de
    grade ``ROUND(lat/lon, GEO_GRID_PRECISION)`` (~1,1 km), limitadas ao
    top :data:`TOP_ZONES_LIMIT`.

    Args:
        start: Data inicial (inclusiva).
        end: Data final (inclusiva).
        base: Se informado, filtra pela coluna Base do CSV.

    Returns:
        Contagens por hora (``00``–``23``), por dia da semana (``0``=domingo
        a ``6``=sábado, padrão SQLite) e as zonas de maior demanda.
    """
    where = "pickup_date >= ? AND pickup_date <= ?"
    args: list[Any] = [start.isoformat(), end.isoformat()]
    if base:
        where += " AND base = ?"
        args.append(base)

    with _db_lock:
        by_hour = {
            str(h): int(c)
            for h, c in _conn.execute(
                f"SELECT strftime('%H', pickup_dt) h, COUNT(*) FROM pickups "
                f"WHERE {where} GROUP BY h",
                args,
            ).fetchall()
        }
        by_weekday = {
            str(wd): int(c)
            for wd, c in _conn.execute(
                f"SELECT strftime('%w', pickup_date) wd, COUNT(*) FROM pickups "
                f"WHERE {where} GROUP BY wd",
                args,
            ).fetchall()
        }
        zones = _conn.execute(
            f"""SELECT ROUND(lat, {GEO_GRID_PRECISION}) glat,
                       ROUND(lon, {GEO_GRID_PRECISION}) glon,
                       COUNT(*) c
                FROM pickups
                WHERE {where} AND lat IS NOT NULL AND lon IS NOT NULL
                GROUP BY glat, glon
                ORDER BY c DESC
                LIMIT {TOP_ZONES_LIMIT}""",
            args,
        ).fetchall()

    return {
        "by_hour": by_hour,
        "by_weekday": by_weekday,
        "top_zones": [
            {"lat": float(lat), "lon": float(lon), "count": int(c)}
            for lat, lon, c in zones
            if lat is not None and lon is not None
        ],
    }


def merge_insights(parts: list[LocalInsightsResult]) -> LocalInsightsResult:
    """Mescla vários :class:`LocalInsightsResult` em um único resumo agregado.

    Soma ``by_hour`` e ``by_weekday`` por chave. Para ``top_zones``, soma
    contagens por célula ``(lat, lon)`` repetida entre nós e reordena,
    mantendo as :data:`TOP_ZONES_LIMIT` maiores no total.

    Limitação: como cada nó já manda só o próprio top-N, uma zona pequena
    em todos os nós individualmente mas grande na soma pode não aparecer —
    aceitável para o propósito exploratório deste endpoint.

    Args:
        parts: Lista de insights parciais (local + vizinhos).

    Returns:
        Insights consolidados com os mesmos campos de :class:`LocalInsightsResult`.
    """
    by_hour: dict[str, int] = {}
    by_weekday: dict[str, int] = {}
    zone_totals: dict[tuple[float, float], int] = {}

    for res in parts:
        for h, c in res.get("by_hour", {}).items():
            by_hour[h] = by_hour.get(h, 0) + int(c)
        for wd, c in res.get("by_weekday", {}).items():
            by_weekday[wd] = by_weekday.get(wd, 0) + int(c)
        for zone in res.get("top_zones", []):
            key = (float(zone["lat"]), float(zone["lon"]))
            zone_totals[key] = zone_totals.get(key, 0) + int(zone["count"])

    top = sorted(zone_totals.items(), key=lambda item: item[1], reverse=True)[:TOP_ZONES_LIMIT]
    return {
        "by_hour": by_hour,
        "by_weekday": by_weekday,
        "top_zones": [
            {"lat": lat, "lon": lon, "count": count} for (lat, lon), count in top
        ],
    }


def _port_from_url(url: str) -> Optional[int]:
    """Extrai a porta TCP final de uma URL tipo ``http://host:porta``.

    Args:
        url: URL de um servidor conhecido (ex.: ``http://192.168.0.176:8002``).

    Returns:
        A porta como ``int``, ou ``None`` se não for possível parsear.
    """
    try:
        return int(url.rstrip("/").rsplit(":", 1)[-1])
    except ValueError:
        return None


def _split_relevant_servers(start: date, end: date) -> tuple[list[str], list[str]]:
    """Separa ``KNOWN_SERVERS`` em relevantes e ignorados para ``[start, end]``.

    Um vizinho é relevante quando o mês de sua partição (conforme a config
    estática :data:`NODES`) se sobrepõe ao intervalo consultado. A decisão é
    tomada localmente, sem nenhuma chamada de rede extra — o coordenador já
    conhece o intervalo de cada partição do cluster (passo 4 do fluxo
    distribuído: "verifica, via configuração..., quais outros servidores
    podem ter dados relevantes").

    Se a porta de uma URL não corresponder a nenhum nó em ``NODES`` (ex.:
    deployment customizado com outro mapeamento porta→mês), o vizinho é
    mantido como relevante por precaução — só filtramos o que temos certeza.

    Args:
        start: Data inicial da consulta (inclusiva).
        end: Data final da consulta (inclusiva).

    Returns:
        Tupla ``(urls_relevantes, server_ids_ignorados)``.
    """
    relevant: list[str] = []
    skipped: list[str] = []
    for url in KNOWN_SERVERS:
        port = _port_from_url(url)
        node_cfg = NODES.get(port) if port is not None else None
        if node_cfg is None:
            relevant.append(url)
            continue
        node_start = date.fromisoformat(node_cfg["date_start"])
        node_end = date.fromisoformat(node_cfg["date_end"])
        if node_start <= end and node_end >= start:
            relevant.append(url)
        else:
            skipped.append(node_cfg["server_id"])
    return relevant, skipped


@app.on_event("startup")
def startup() -> None:
    """Hook de startup: abre/importa o SQLite da partição deste nó."""
    load_data()


@app.get("/", include_in_schema=False)
def home() -> FileResponse:
    """Serve a interface web estática (``static/index.html``).

    Raises:
        HTTPException: Se o arquivo HTML não existir (status 404).
    """
    path = STATIC_DIR / "index.html"
    if not path.exists():
        raise HTTPException(404, "static/index.html não encontrado")
    return FileResponse(path)


@app.get("/health")
def health() -> HealthResponse:
    """Retorna status do nó, partição e quantidade de registros carregados."""
    return {
        "status": "ok",
        "server_id": SERVER_ID,
        "partition": NODE["partition"],
        "records": RECORD_COUNT,
    }


@app.get("/metadata")
def metadata() -> MetadataResponse:
    """Retorna metadados do nó (intervalo de dados que possui) e vizinhos conhecidos."""
    return {
        "server_id": SERVER_ID,
        "owns": {
            "date_start": NODE["date_start"],
            "date_end": NODE["date_end"],
            "partition_description": NODE["partition"],
        },
        "known_servers": KNOWN_SERVERS,
    }


@app.get("/cluster/status")
async def cluster_status(request: Request) -> ClusterStatusResponse:
    """Sonda ``/health`` nos vizinhos e monta o panorama do cluster.

    O nó atual é sempre marcado como online; falhas de rede nos vizinhos
    aparecem com ``online: false``. A URL deste nó é derivada de
    ``request.base_url``, refletindo o host/porta reais usados pelo
    cliente para chegar até aqui — em vez de assumir ``localhost``, o que
    seria incorreto quando o nó roda em outra máquina da rede.

    Args:
        request: Requisição HTTP recebida (usada só para extrair o host
            pelo qual este nó foi acessado).
    """
    self_url = str(request.base_url).rstrip("/")
    nodes: list[ClusterNodeStatus] = [
        {
            "url": self_url,
            "server_id": SERVER_ID,
            "online": True,
            "self": True,
            "partition": NODE["partition"],
        }
    ]

    async def probe(server_url: str) -> ClusterNodeStatus:
        """Consulta ``/health`` de um vizinho e devolve o status tipado."""
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
) -> LocalSummaryResponse:
    """Resume embarques apenas com dados locais deste nó.

    Args:
        start_date: Data inicial no formato ``YYYY-MM-DD``.
        end_date: Data final no formato ``YYYY-MM-DD``.
        base: Filtro opcional por código TLC ou nome da base.

    Raises:
        HTTPException: Datas inválidas ou ``start_date`` > ``end_date``.
    """
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    base_clean = resolve_base_filter(base)
    return {
        "server_id": SERVER_ID,
        "scope": "local",
        "complete": True,
        "result": summarize(start, end, base_clean),
    }


@app.get("/summary")
async def distributed_summary(
    start_date: str = Query(...),
    end_date: str = Query(...),
    base: Optional[str] = Query(None),
) -> DistributedSummaryResponse:
    """Coordena consulta distribuída agregando ``/local/summary`` dos vizinhos.

    Calcula o resumo local, filtra ``KNOWN_SERVERS`` para os vizinhos cujo
    mês se sobrepõe a ``[start_date, end_date]`` (via :func:`_split_relevant_servers`,
    sem chamada de rede extra) e solicita o mesmo intervalo só a esses,
    mesclando tudo com :func:`merge_results`. Vizinhos fora do intervalo
    nem são contatados; vizinhos relevantes porém inacessíveis entram em
    ``failed_servers`` e ``complete`` fica ``False``.

    Args:
        start_date: Data inicial no formato ``YYYY-MM-DD``.
        end_date: Data final no formato ``YYYY-MM-DD``.
        base: Filtro opcional por código TLC ou nome da base.

    Raises:
        HTTPException: Datas inválidas ou ``start_date`` > ``end_date``.
    """
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    base_clean = resolve_base_filter(base)
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if base_clean:
        params["base"] = base_clean

    local = summarize(start, end, base_clean)
    contacted: list[str] = [SERVER_ID]
    failed: list[str] = []
    parts: list[SummaryResult] = [local]

    async def fetch(
        url: str,
    ) -> tuple[Optional[SummaryResult], str, Optional[str]]:
        """Busca ``/local/summary`` em ``url``.

        Returns:
            Tupla ``(resultado, referência_do_servidor, erro)``. Em sucesso
            ``erro`` é ``None``; em falha ``resultado`` é ``None``.
        """
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(f"{url.rstrip('/')}/local/summary", params=params)
                r.raise_for_status()
                data = r.json()
                return data.get("result"), data.get("server_id") or url, None
        except Exception as exc:
            return None, url, str(exc)

    relevant_urls, _skipped = _split_relevant_servers(start, end)
    for result, server_ref, err in await asyncio.gather(*[fetch(u) for u in relevant_urls]):
        if err or result is None:
            # tenta nome pela porta conhecida
            node_cfg = NODES.get(_port_from_url(server_ref) or -1)
            failed.append(node_cfg["server_id"] if node_cfg else server_ref)
        else:
            contacted.append(server_ref)
            parts.append(result)

    return {
        "scope": "distributed",
        "coordinator": SERVER_ID,
        "query": {"start_date": start_date, "end_date": end_date, "base": base_clean},
        "complete": len(failed) == 0,
        "servers_contacted": contacted,
        "failed_servers": failed,
        "result": merge_results(parts),
    }


@app.get("/local/insights")
def local_insights(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    base: Optional[str] = Query(None),
) -> LocalInsightsResponse:
    """Cruzamentos locais: demanda por hora, dia da semana e zona geográfica.

    Endpoint opcional (métricas extras do PDF §7). Não altera
    ``/local/summary`` nem o schema obrigatório.

    Args:
        start_date: Data inicial no formato ``YYYY-MM-DD``.
        end_date: Data final no formato ``YYYY-MM-DD``.
        base: Filtro opcional por código TLC ou nome da base.

    Raises:
        HTTPException: Datas inválidas ou ``start_date`` > ``end_date``.
    """
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    base_clean = resolve_base_filter(base)
    return {
        "server_id": SERVER_ID,
        "scope": "local",
        "complete": True,
        "result": insights(start, end, base_clean),
    }


@app.get("/insights")
async def distributed_insights(
    start_date: str = Query(...),
    end_date: str = Query(...),
    base: Optional[str] = Query(None),
) -> InsightsResponse:
    """Coordena insights distribuídos agregando ``/local/insights`` dos vizinhos.

    Mesmo padrão de :func:`distributed_summary`: filtra vizinhos relevantes,
    chama só ``/local/insights`` (sem ciclo) e mescla com :func:`merge_insights`.

    Args:
        start_date: Data inicial no formato ``YYYY-MM-DD``.
        end_date: Data final no formato ``YYYY-MM-DD``.
        base: Filtro opcional por código TLC ou nome da base.

    Raises:
        HTTPException: Datas inválidas ou ``start_date`` > ``end_date``.
    """
    start, end = parse_date(start_date, "start_date"), parse_date(end_date, "end_date")
    if start > end:
        raise HTTPException(400, "start_date não pode ser maior que end_date.")

    base_clean = resolve_base_filter(base)
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if base_clean:
        params["base"] = base_clean

    local = insights(start, end, base_clean)
    contacted: list[str] = [SERVER_ID]
    failed: list[str] = []
    parts: list[LocalInsightsResult] = [local]

    async def fetch(
        url: str,
    ) -> tuple[Optional[LocalInsightsResult], str, Optional[str]]:
        """Busca ``/local/insights`` em ``url``."""
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(f"{url.rstrip('/')}/local/insights", params=params)
                r.raise_for_status()
                data = r.json()
                return data.get("result"), data.get("server_id") or url, None
        except Exception as exc:
            return None, url, str(exc)

    relevant_urls, _skipped = _split_relevant_servers(start, end)
    for result, server_ref, err in await asyncio.gather(*[fetch(u) for u in relevant_urls]):
        if err or result is None:
            node_cfg = NODES.get(_port_from_url(server_ref) or -1)
            failed.append(node_cfg["server_id"] if node_cfg else server_ref)
        else:
            contacted.append(server_ref)
            parts.append(result)

    return {
        "scope": "distributed",
        "coordinator": SERVER_ID,
        "query": {"start_date": start_date, "end_date": end_date, "base": base_clean},
        "complete": len(failed) == 0,
        "servers_contacted": contacted,
        "failed_servers": failed,
        "result": merge_insights(parts),
    }
