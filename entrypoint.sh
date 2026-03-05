#!/bin/bash
set -e

# DB setup
alembic upgrade head  # single head for now, see https://github.com/procrastinate-org/procrastinate/issues/1040#issuecomment-4000763991

# execute CMD
exec "$@"
