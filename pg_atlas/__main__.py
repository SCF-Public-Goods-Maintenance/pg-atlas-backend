"""
Entry point for `python -m pg_atlas` and the `pg-atlas` CLI command.

Passes ``--reload`` to enable hot-reload in development. For production,
run uvicorn directly (reload is disabled):

    uvicorn pg_atlas.main:app --host 0.0.0.0 --port 8000
"""

import argparse

import uvicorn


def main() -> None:
    """
    Start the PG Atlas API server using uvicorn.

    Flags:
        --reload: Enable uvicorn hot-reload (default: off).
        --host: Bind address (default: 127.0.0.1).
        --port: Listen port (default: 8000).
    """
    parser = argparse.ArgumentParser(description="PG Atlas API server")
    parser.add_argument("--reload", action="store_true", help="Enable hot-reload (development only)")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Listen port (default: 8000)")
    args = parser.parse_args()

    uvicorn.run(
        "pg_atlas.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
