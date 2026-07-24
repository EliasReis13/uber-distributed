#!/usr/bin/env python3
"""Sobe / para o cluster Uber (6 servidores).

Uso
---
::

    python start.py       # abre 6 janelas de terminal, uma por servidor (8001–8006)
    python start.py stop  # encerra
    python start.py 1     # sobe só o servidor 01, no terminal atual

Para implantação em rede real (uma máquina por servidor), use
``python start.py N`` em cada máquina (ver README.md, seção "Rodando em
máquinas diferentes na mesma rede").

``SERVER_PORT``, ``KNOWN_SERVERS`` e ``HTTP_TIMEOUT`` podem vir de um
arquivo ``.env`` na raiz do projeto (copie ``.env.example``), em vez de
exportar variáveis manualmente a cada terminal aberto.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT: Path = Path(__file__).resolve().parent
PORTS: list[int] = [8001, 8002, 8003, 8004, 8005, 8006]

# Carrega o ``.env`` da raiz (se existir) para dentro de ``os.environ`` deste
# processo, ANTES de qualquer subprocesso ser criado — assim ``start_one()``
# e ``open_terminal_for()``, que copiam ``os.environ``, repassam KNOWN_SERVERS
# / HTTP_TIMEOUT do ``.env`` para o uvicorn. Não sobrescreve variáveis já
# exportadas manualmente no shell (override=False por padrão).
load_dotenv(ROOT / ".env")


def _uvicorn_command(port: int) -> str:
    """Monta a linha de comando (string) que sobe o uvicorn na ``port``."""
    return f'"{sys.executable}" -m uvicorn server.app:app --host 0.0.0.0 --port {port} --log-level info'


def open_terminal_for(port: int) -> bool:
    """Abre uma nova janela/aba de terminal rodando o nó ``port``.

    Tenta, em ordem: Windows (``cmd /c start``), macOS (``osascript`` +
    Terminal.app) e Linux (``gnome-terminal``/``x-terminal-emulator``/``xterm``).

    Returns:
        ``True`` se conseguiu abrir uma janela nova; ``False`` se nenhum
        mecanismo de terminal foi encontrado (o chamador deve then usar o
        fallback em background).
    """
    title = f"servidor_{port - 8000:02d}"
    server_cmd = _uvicorn_command(port)

    if sys.platform == "win32":
        set_env = f"set SERVER_PORT={port}&&"
        inner = f'{set_env}{server_cmd}'
        subprocess.Popen(
            f'cmd /c start "{title}" cmd /k "{inner}"',
            cwd=ROOT,
            shell=True,
        )
        return True

    if sys.platform == "darwin":
        env_prefix = f"export SERVER_PORT={port};"
        script = f'cd {ROOT}; {env_prefix} {server_cmd}'
        osa = (
            'tell application "Terminal" to do script '
            f'"{script}"'
        )
        try:
            subprocess.Popen(["osascript", "-e", osa])
            return True
        except OSError:
            return False

    # Linux / demais Unix: tenta emuladores de terminal comuns.
    env = os.environ.copy()
    env["SERVER_PORT"] = str(port)
    for emulator, args in (
        ("gnome-terminal", ["--title", title, "--", "bash", "-c", f"{server_cmd}; exec bash"]),
        ("x-terminal-emulator", ["-T", title, "-e", "bash", "-c", f"{server_cmd}; exec bash"]),
        ("xterm", ["-T", title, "-e", "bash", "-c", f"{server_cmd}; exec bash"]),
    ):
        path = shutil.which(emulator)
        if path is None:
            continue
        try:
            subprocess.Popen([path, *args], cwd=ROOT, env=env)
            return True
        except OSError:
            continue
    return False


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
    """Encerra processos nas portas do cluster (8001–8006).

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
        print("Nenhum servidor ativo nas portas 8001-8006.")
    else:
        print(f"Encerrados {len(killed)} processo(s).")


def start_one(port: int, *, foreground: bool = False) -> subprocess.Popen[bytes] | None:
    """Inicia um nó uvicorn na porta indicada.

    Define ``SERVER_PORT`` no ambiente do subprocesso. Se a porta já estiver
    em uso, apenas registra e retorna ``None``.

    Args:
        port: Porta do nó (8001 a 8006).
        foreground: Se ``True``, bloqueia no processo (útil para um nó só);
            se ``False``, sobe em background via :class:`subprocess.Popen`.

    Returns:
        Handle do processo em background, ou ``None`` se pulou / foreground.
    """
    existing = pid_on_port(port)
    if existing is not None:
        print(f"  porta {port} ja em uso (PID {existing}) - pulando")
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
    """Sobe os seis nós, cada um em sua própria janela de terminal.

    Se alguma porta do cluster já estiver ocupada, chama :func:`stop_all`
    antes de reiniciar. Se não for possível abrir janelas novas (SO sem
    terminal reconhecido), cai no modo antigo: sobe tudo em background no
    terminal atual e aguarda Ctrl+C para encerrar.
    """
    if any(pid_on_port(p) for p in PORTS):
        print("Liberando portas ocupadas...")
        stop_all()
        time.sleep(1)

    print("Iniciando cluster (6 servidores, uma janela cada)...\n")
    opened = 0
    for port in PORTS:
        if pid_on_port(port) is not None:
            print(f"  porta {port} ja em uso - pulando")
            continue
        title = f"servidor_{port - 8000:02d}"
        if open_terminal_for(port):
            print(f"  > {title} -> nova janela -> http://localhost:{port}/")
            opened += 1
        else:
            print("  Nenhum terminal grafico encontrado; usando modo em segundo plano.")
            _start_all_background()
            return

    if opened:
        print("\nPronto. Interface: http://localhost:8001/")
        print("Para encerrar: python start.py stop\n")


def _start_all_background() -> None:
    """Modo antigo: sobe os nós que faltam em background no mesmo terminal.

    Usado como fallback quando nenhum emulador de terminal é encontrado.
    Bloqueia até Ctrl+C, encerrando os processos filhos ao sair.
    """
    procs: list[subprocess.Popen[bytes]] = [
        p for p in (start_one(port) for port in PORTS if pid_on_port(port) is None) if p is not None
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
        print("\nEncerrando...")
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def main() -> None:
    """CLI: sem args sobe o cluster; ``stop`` / ``1``..``6`` / ``help``."""
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
    if cmd in {"1", "2", "3", "4", "5", "6"}:
        start_one(8000 + int(cmd), foreground=True)
        return

    sys.exit(f"Uso: python start.py [stop|1|2|3|4|5|6]\n{__doc__}")


if __name__ == "__main__":
    main()
