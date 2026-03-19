"""``haute serve`` command."""

import signal
import subprocess
import sys
from pathlib import Path

import click

from haute.cli._helpers import _find_frontend_dir, _node_env, _npm, _open_browser


@click.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", default=8000, type=int, help="Backend API port.")
@click.option("--no-browser", is_flag=True, help="Don't open browser automatically.")
def serve(host: str, port: int, no_browser: bool) -> None:
    """Start the Haute UI server."""
    import uvicorn

    from haute.server import STATIC_DIR

    frontend_dir = _find_frontend_dir()
    dev_mode = frontend_dir is not None and (frontend_dir / "node_modules").exists()

    if dev_mode:
        click.echo("[dev] Dev mode: starting Vite dev server + FastAPI backend")
        click.echo("  Frontend -> http://localhost:5173  (open this)")
        click.echo(f"  Backend  -> http://{host}:{port}   (API only)")
        click.echo("")
        vite_proc = subprocess.Popen(
            [_npm(), "run", "dev"],
            cwd=str(frontend_dir),
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=_node_env(),
        )

        def _cleanup(signum: int, frame: object) -> None:
            vite_proc.terminate()
            sys.exit(0)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        if not no_browser:
            import threading

            threading.Timer(2.0, _open_browser, args=("http://localhost:5173",)).start()

        # Resolve the haute package directory so uvicorn only reloads on
        # server source changes, not on user pipeline file writes.
        import haute as _haute_pkg

        _haute_src_dir = str(Path(_haute_pkg.__file__).resolve().parent)

        try:
            uvicorn.run(
                "haute.server:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[_haute_src_dir],
                log_level="warning",
            )
        finally:
            vite_proc.terminate()
    else:
        if not STATIC_DIR.exists():
            click.echo(
                "Error: No built frontend found. "
                "Run 'npm run build' in frontend/ first, or "
                "install node_modules for dev mode.",
                err=True,
            )
            raise SystemExit(1)

        if not no_browser:
            import threading

            threading.Timer(1.5, _open_browser, args=(f"http://{host}:{port}",)).start()

        uvicorn.run(
            "haute.server:app",
            host=host,
            port=port,
        )
