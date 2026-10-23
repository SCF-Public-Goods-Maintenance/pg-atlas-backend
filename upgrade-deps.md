# A recipe for upgrading all dependencies

We aim to keep PG Atlas compatible with the latest versions of all dependencies. Sometimes the
pragmatic choice is to temporarily introduce an upper version constraint, for instance when the new
version of a package would lead to unresolvable transitive dependencies, or when a wheel is not yet
available for the latest stable Python release. Upper bounds should always be reevaluated during the
upgrade flow, and be removed if possible.

This recipe describes the flow in which all dependencies are upgraded simultaneously. If selective
upgrades are desired, you should adjust commands, and pick and choose the relevant parts.

## Upgrade to latest within the install spec

Compare locked/installed against latest available:

```sh
uvx uv-outdated
```

Lock and install latest versions:

```sh
uv sync --upgrade
```

## Recompile deps.dev proto

If `betterproto2-compiler` has been upgraded: follow the instructions in
`pg_atlas/deps_dev/generate_async_client.py`. After the build is successful, reinstall the `dev`
group:

```sh
uv sync --group dev
```

## Generate vendored type stubs

```sh
# generate aiobotocore services
uvx --with 'aiobotocore==x.y.z' mypy-boto3-builder ./vendored --download-static-stubs --product aiobotocore-custom --output-type wheel --services s3

# install with uv
uv add --dev vendored/types_aiobotocore_custom-x.y.z-py3-none-any.whl
```

Remove the older `vendored/*.whl` stubs that have now become obsolete.

## Run full test suite

Include database-backed tests but let live API tests skip. Use the IDE test runner, or:

```sh
uv run pytest
```

Assess how to follow up on any failing tests.

1. Which upgrade causes the test failure?
2. Do changelogs and migration guides provide enough info to make compatibility adjustments?
3. If a large refactor is needed: does an upper bound need to be introduced?

## Linting

Ruff upgrades can cause violations in unmodified code. Fix the linter errors.

```sh
uv run ruff check .
uv run ruff format --check .
```

## Type checking

Newer type checkers can catch pre-existing problems.

```sh
uv run mypy pg_atlas/
uv run basedpyright pg_atlas/
```

Sometimes the fix is a better type annotation. Other cases may be actual logic errors that can cause
runtime problems. If you are time constrained: leave the problem visible instead of suppressing it.

When an upgraded dependency introduces type-checking errors, consult its documentation for the
installed version and compare the relevant signatures with the previous version. Use that evidence
to identify whether the API or its inferred types changed, then make the smallest type-correct
adjustment that preserves runtime behavior. Prefer precise annotations and existing expression
methods over repeated branches or type suppressions, and validate the affected module and behavior
with focused checks.
