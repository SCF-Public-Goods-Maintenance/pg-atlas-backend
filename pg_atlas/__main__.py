"""Entry point for `python -m pg_atlas`.

Launches the uvicorn development server. For production, run uvicorn directly:

    uvicorn pg_atlas.main:app --host 0.0.0.0 --port 8000
"""

import uvicorn


def main() -> None:
    """Start the PG Atlas API server using uvicorn."""
    uvicorn.run(
        "pg_atlas.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
