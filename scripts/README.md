# scripts/

The scripts in this directory are intended for human-in-the-loop usage. They are the automations that achieve part of a larger task. Each script starts with a docstring that explains its purpose and gives basic usage instructions.

Python scripts import from `pg_atlas`, never the other way around. They are executed in the same virtualenv with `uv run ...`, and _may_ be registered in `pyproject.toml` under `[project.scripts]`.
