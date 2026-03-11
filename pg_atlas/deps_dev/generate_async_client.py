"""
Generate async gRPC client stubs from the deps.dev proto definition.

Requires the ``proto-build`` dependency group::

    uv sync --group proto-build
    uv run python pg_atlas/deps_dev/generate_async_client.py

The generated code is written to ``pg_atlas/deps_dev/lib/`` and MUST be
committed to the repository so that normal ``uv sync`` (without the
``proto-build`` group) is sufficient to import the client at runtime.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_PATH: Path = Path(__file__).resolve()
PROTO_DIR: Path = SCRIPT_PATH.parent / "protos"
PROTO_FILE: Path = PROTO_DIR / "api-v3alpha.proto"
OUTPUT_DIR: Path = SCRIPT_PATH.parent / "lib"


def generate() -> int:
    """
    Compile the deps.dev proto into async Python stubs via betterproto2.

    Returns 0 on success, 1 on error.
    """

    if not PROTO_FILE.exists():
        print(
            f"❌ Proto file not found: {PROTO_FILE}\n   Run sync_proto.py first to download it.",
            file=sys.stderr,
        )

        return 1

    # Import grpc_tools lazily — only available with the proto-build group.
    try:
        import grpc_tools
        from grpc_tools import protoc
    except ImportError:
        print(
            "❌ grpc_tools is not installed.\n"
            "   Install the proto-build dependency group:\n"
            "       uv sync --group proto-build",
            file=sys.stderr,
        )

        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # grpc_tools ships google/protobuf/*.proto — we need its include path
    # so that ``import "google/protobuf/timestamp.proto"`` resolves.
    grpc_tools_proto_dir = Path(grpc_tools.__file__).resolve().parent / "_proto"

    # ── invoke protoc programmatically ────────────────────────────────
    # Equivalent CLI:
    #   python -m grpc_tools.protoc \
    #       -I pg_atlas/deps_dev/protos \
    #       -I <grpc_tools>/_proto \
    #       --python_betterproto2_out=pg_atlas/deps_dev/lib \
    #       pg_atlas/deps_dev/protos/api-v3alpha.proto
    #
    # The local protos/ tree also contains google/api/{annotations,http}.proto
    # vendored from googleapis so no extra download is needed at build time.
    args = [
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"-I{grpc_tools_proto_dir}",
        f"--python_betterproto2_out={OUTPUT_DIR}",
        str(PROTO_FILE),
    ]

    print(f"🔧 Compiling {PROTO_FILE.name} → {OUTPUT_DIR}/")
    exit_code: int = protoc.main(args)

    if exit_code != 0:
        print(f"❌ protoc exited with code {exit_code}", file=sys.stderr)

        return 1

    print("✅ Client stubs generated successfully.")

    return 0


if __name__ == "__main__":
    sys.exit(generate())
