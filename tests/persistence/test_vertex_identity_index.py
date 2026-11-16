"""
Schema regression for case-insensitive repository vertex identity.
"""

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from pg_atlas.db_models.repo_vertex import RepoVertex


def test_vertex_identity_is_unique_case_insensitively() -> None:
    """
    Match the identity expression used in lookups with a unique database index.
    """

    index = next(item for item in RepoVertex.__table__.indexes if item.name == "uq_repo_vertices_canonical_id_lower")
    assert index.unique
    assert "UNIQUE INDEX uq_repo_vertices_canonical_id_lower ON repo_vertices (lower(canonical_id))" in str(
        CreateIndex(index).compile(dialect=postgresql.dialect())
    )
