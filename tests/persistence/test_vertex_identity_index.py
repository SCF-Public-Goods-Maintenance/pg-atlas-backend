"""
Schema regression for case-insensitive repository vertex identity.
"""

from sqlalchemy import func
from sqlalchemy.sql.elements import ClauseElement

from pg_atlas.db_models.repo_vertex import RepoVertex


def test_vertex_identity_is_unique_case_insensitively() -> None:
    """
    Require uniqueness of lowercase canonical IDs independently of index naming.
    """

    indexes = RepoVertex.metadata.tables["repo_vertices"].indexes
    identity_expression = func.lower(RepoVertex.canonical_id)
    assert any(
        index.unique
        and len(index.expressions) == 1
        and isinstance(index.expressions[0], ClauseElement)
        and index.expressions[0].compare(identity_expression)
        for index in indexes
    )
