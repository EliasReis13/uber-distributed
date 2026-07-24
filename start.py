#!/usr/bin/env python3
"""Sobe / para o cluster Uber (3 servidores).

Uso
---
::

    python start.py       # sobe 8001–8003
    python start.py stop  # encerra
    python start.py 1     # sobe só o servidor 01
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parent
PORTS: list[int] = [8001, 8002, 8003]


def pid_on_port(port: int) -> int | None:
    """Retorna o PID do processo em escuta na ``port``, ou ``None``.

    No Windows usa ``netstat -ano``; em Unix usa ``lsof -ti tcp:<port>``.

    Args:
        port: Porta TCP a inspecionar.

    Returns:
        PID do listener, se encontrado; caso contrário ``None``.
    """
    if sys.platform == "win32":
        try:
            out = subprocess.check_output(["netstat", "-ano"], text=True, errors="ignore")
        except (OSError, subprocess.CalledProcessError):
            return None
        for line in out.splitlines():
            if f":{port}" not in line or "LISTENING" not in line:
                continue
            parts = line.split()
            if parts and parts[-1].isdigit():
                return int(parts[-1])
        return None

    try:
        out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}"], text=True, errors="ignore")
        for token in out.split():
            if token.isdigit():
                return int(token)
    except (OSError, subprocess.CalledProcessError):
        pass
    return None


def stop_all() -> None:
    """Encerra processos nas portas do cluster (8001–8003).

    Windows: ``taskkill /F``. Unix: ``SIGTERM``.
    """
    killed: list[int] = []
    for port in PORTS:
        pid = pid_on_port(port)
        if pid is None:
            continue
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True)
            else:
                os.kill(pid, signal.SIGTERM)
            killed.append(pid)
            print(f"  porta {port}: encerrado PID {pid}")
        except OSError as exc:
            print(f"  porta {port}: falha ao encerrar PID {pid}: {exc}")
    if not killed:
        print("Nenhum servidor ativo nas portas 8001–8003.")
    else:
        print(f"Encerrados {len(killed)} processo(s).")


def start_one(port: int, *, foreground: bool = False) -> subprocess.Popen[bytes] | None:
    """Inicia um nó uvicorn na porta indicada.

    Define ``SERVER_PORT`` no ambiente do subprocesso. Se a porta já estiver
    em uso, apenas registra e retorna ``None``.

    Args:
        port: Porta do nó (8001, 8002 ou 8003).
        foreground: Se ``True``, bloqueia no processo (útil para um nó só);
            se ``False``, sobe em background via :class:`subprocess.Popen`.

    Returns:
        Handle do processo em background, ou ``None`` se pulou / foreground.
    """
    existing = pid_on_port(port)
    if existing is not None:
        print(f"  porta {port} já em uso (PID {existing}) — pulando")
        return None

    env = os.environ.copy()
    env["SERVER_PORT"] = str(port)
    env.setdefault("PYTHONUNBUFFERED", "1")

    cmd: list[str] = [
        sys.executable, "-m", "uvicorn", "server.app:app",
        "--host", "0.0.0.0", "--port", str(port), "--log-level", "info",
    ]
    print(f"  > servidor_{port - 8000:02d} -> http://localhost:{port}/")

    if foreground:
        subprocess.run(cmd, cwd=ROOT, env=env)
        return None
    return subprocess.Popen(cmd, cwd=ROOT, env=env)


def start_all() -> None:
    """Sobe os três nós e aguarda até Ctrl+C ou término dos processos.

    Se alguma porta do cluster já estiver ocupada, chama :func:`stop_all`
    antes de reiniciar.
    """
    if any(pid_on_port(p) for p in PORTS):
        print("Liberando portas ocupadas…")
        stop_all()
        time.sleep(1)

    print("Iniciando cluster (3 servidores)…\n")
    procs: list[subprocess.Popen[bytes]] = [
        p for p in (start_one(port) for port in PORTS) if p is not None
    ]

    print("\nPronto. Interface: http://localhost:8001/")
    print("Para encerrar: python start.py stop   (ou Ctrl+C)\n")
    if not procs:
        return

    try:
        while any(p.poll() is None for p in procs):
            time.sleep(1)
        print("Todos os processos terminaram.")
    except KeyboardInterrupt:
        print("\nEncerrando…")
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def main() -> None:
    """CLI: sem args sobe o cluster; ``stop`` / ``1``|``2``|``3`` / ``help``."""
    os.chdir(ROOT)
    args = sys.argv[1:]

    if not args:
        start_all()
        return

    cmd = args[0].lower()
    if cmd in {"stop", "kill", "down"}:
        stop_all()
        return
    if cmd in {"help", "-h", "--help"}:
        print(__doc__)
        return
    if cmd in {"1", "2", "3"}:
        start_one(8000 + int(cmd), foreground=True)
        return

    sys.exit(f"Uso: python start.py [stop|1|2|3]\n{__doc__}")


if __name__ == "__main__":
    main()
