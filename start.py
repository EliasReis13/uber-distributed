#!/usr/bin/env python3
"""Inicia / para os servidores do cluster Uber.

Uso:
  python start.py          # sobe os 6 servidores
  python start.py stop     # encerra processos nas portas 8001–8006
  python start.py 1        # sobe só o servidor 01 (também: 2 … 6)
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"
PORTS = list(range(8001, 8007))


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def config_files() -> list[Path]:
    files = sorted(CONFIG_DIR.glob("servidor_*.env"))
    if not files:
        sys.exit(f"Nenhum config encontrado em {CONFIG_DIR}")
    return files


def pid_on_port(port: int) -> int | None:
    if sys.platform == "win32":
        try:
            out = subprocess.check_output(
                ["netstat", "-ano"], text=True, errors="ignore"
            )
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
        out = subprocess.check_output(
            ["lsof", "-ti", f"tcp:{port}"], text=True, errors="ignore"
        )
        for token in out.split():
            if token.isdigit():
                return int(token)
    except (OSError, subprocess.CalledProcessError):
        pass
    return None


def stop_all() -> None:
    killed: list[int] = []
    for port in PORTS:
        pid = pid_on_port(port)
        if pid is None:
            continue
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F"],
                    check=False,
                    capture_output=True,
                )
            else:
                os.kill(pid, signal.SIGTERM)
            killed.append(pid)
            print(f"  porta {port}: encerrado PID {pid}")
        except OSError as exc:
            print(f"  porta {port}: falha ao encerrar PID {pid}: {exc}")
    if not killed:
        print("Nenhum servidor ativo nas portas 8001–8006.")
    else:
        print(f"Encerrados {len(killed)} processo(s).")


def start_one(env_path: Path, *, foreground: bool = False) -> subprocess.Popen | None:
    cfg = load_env(env_path)
    port = int(cfg.get("SERVER_PORT", "0"))
    server_id = cfg.get("SERVER_ID", env_path.stem)

    existing = pid_on_port(port)
    if existing is not None:
        print(f"  {server_id}: porta {port} já em uso (PID {existing}) — pulando")
        return None

    child_env = os.environ.copy()
    child_env.update(cfg)
    child_env.setdefault("PYTHONUNBUFFERED", "1")

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "server.app:app",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--log-level",
        "info",
    ]

    print(f"  ▶ {server_id} → http://localhost:{port}/")

    if foreground:
        subprocess.run(cmd, cwd=ROOT, env=child_env)
        return None

    return subprocess.Popen(cmd, cwd=ROOT, env=child_env)


def start_all() -> None:
    occupied = [p for p in PORTS if pid_on_port(p) is not None]
    if occupied:
        print("Liberando portas ocupadas…")
        stop_all()
        time.sleep(1)

    print("Iniciando cluster Uber (6 servidores)…\n")
    procs: list[subprocess.Popen] = []
    for path in config_files():
        proc = start_one(path)
        if proc is not None:
            procs.append(proc)

    print(
        "\nPronto. Interface: http://localhost:8001/\n"
        "Para encerrar: python start.py stop   (ou Ctrl+C)\n"
    )
    if not procs:
        return

    try:
        while True:
            alive = [p for p in procs if p.poll() is None]
            if not alive:
                print("Todos os processos terminaram.")
                break
            time.sleep(1)
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


def resolve_single(arg: str) -> Path:
    if arg.isdigit():
        path = CONFIG_DIR / f"servidor_{int(arg):02d}.env"
    else:
        name = arg if arg.endswith(".env") else f"{arg}.env"
        path = CONFIG_DIR / name if (CONFIG_DIR / name).exists() else Path(arg)
    if not path.exists():
        sys.exit(f"Config não encontrado: {path}")
    return path


def main() -> None:
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

    env_path = resolve_single(args[0])
    start_one(env_path, foreground=True)


if __name__ == "__main__":
    main()
