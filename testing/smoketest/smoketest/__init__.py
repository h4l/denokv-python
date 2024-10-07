from __future__ import annotations

import asyncio
import os
import secrets
import signal
import socket
import sqlite3
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory

from v8serialize.jstypes import JSObject
from yarl import URL

import denokv


def main() -> None:
    db_sql = files(__name__).joinpath("db.sql").read_text()
    with TemporaryDirectory() as db_dir:
        db_path = Path(db_dir) / "db.sqlite"
        db = sqlite3.connect(db_path)
        db.executescript(db_sql)
        db.close()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(smoketest(db_path))


async def get_unused_port() -> int:
    async with await asyncio.get_running_loop().create_server(
        lambda: asyncio.Protocol(), host="localhost", port=0, family=socket.AF_INET
    ) as server:
        try:
            port = server.sockets[0].getsockname()[1]
            if not isinstance(port, int):
                raise TypeError("expected port to be an int")
            return port
        except Exception as e:
            raise TypeError(
                f"failed to retrieve IPv4 TCP port from asyncio server: {server!r}"
            ) from e


async def smoketest(db_path: Path) -> None:
    token = secrets.token_urlsafe()
    port = await get_unused_port()

    server_addr = f"127.0.0.1:{port:d}"

    server_exit = asyncio.create_task(
        run_denokv_server(addr=server_addr, token=token, db_path=db_path)
    )
    communicate = asyncio.create_task(
        communicate_with_denokv(
            server_url=URL(f"http://{server_addr}"), access_token=token
        )
    )

    try:
        done, pending = await asyncio.wait(
            [server_exit, communicate],
            return_when=asyncio.FIRST_COMPLETED,
            timeout=4,
        )
        if not done:
            raise TimeoutError("Timed out while communicating with denokv server")
    finally:
        server_exit.cancel()
        communicate.cancel()
        await asyncio.wait([server_exit, communicate])
    await server_exit
    await communicate


async def run_denokv_server(*, addr: str, token: str, db_path: Path) -> None:
    env = {
        "DENO_KV_HOST": addr,
        "DENO_KV_ACCESS_TOKEN": token,
        "DENO_KV_SQLITE_PATH": str(db_path.absolute()),
    }
    if path := os.environ.get("PATH"):
        env["PATH"] = path
    denokv_proc = await asyncio.create_subprocess_exec(
        "denokv", "serve", "--addr", addr, env=env
    )

    try:
        returncode = await denokv_proc.wait()
        if returncode == 0:
            return
    except asyncio.CancelledError:
        returncode = await terminate_gracefully(denokv_proc, grace_period=4)
        if returncode == -signal.SIGTERM:
            return

    raise RuntimeError(f"denokv server exited with status: {returncode}")


async def communicate_with_denokv(*, server_url: URL, access_token: str) -> None:
    kv = denokv.open_kv(server_url, access_token=access_token)
    async with kv.session:
        k, entry = await kv.get(("smoketest",))

        if not entry:
            raise ValueError(f"key {k} not found in database")
        if entry.value != JSObject(message="Hello World"):
            raise ValueError(f"Unexpected value for key {k}: {entry}")

    print("Successfully communicated with denokv server.")


async def terminate_gracefully(
    proc: asyncio.subprocess.Process, grace_period: float = 1
) -> int:
    proc.terminate()
    try:
        return await asyncio.wait_for(proc.wait(), timeout=grace_period)
    except asyncio.TimeoutError:
        proc.kill()
    return await proc.wait()


if __name__ == "__main__":
    main()
