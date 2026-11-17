"""
GraphQL payload shapes and query documents for the maintenance collector.

The structs are typed subsets of the GraphQL responses, decoded with msgspec;
the page sizes are interpolated into the query documents so the cap
arithmetic (pages x page size) has one source of truth.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import msgspec

#: GraphQL page sizes, interpolated into the query documents below so the
#: cap arithmetic (pages x page size) has one source of truth.
COHORT_PAGE_SIZE = 25
OPEN_ITEMS_PAGE_SIZE = 100
COMMENTS_FIRST_PAGE_SIZE = 50
CONTINUATION_PAGE_SIZE = 100
REVIEWS_FIRST_PAGE_SIZE = 20
TIMELINE_PAGE_SIZE = 10
PAGE_SIZES = {
    "cohort_page": COHORT_PAGE_SIZE,
    "open_page": OPEN_ITEMS_PAGE_SIZE,
    "comments_first": COMMENTS_FIRST_PAGE_SIZE,
    "continuation": CONTINUATION_PAGE_SIZE,
    "reviews_first": REVIEWS_FIRST_PAGE_SIZE,
    "timeline": TIMELINE_PAGE_SIZE,
}


# ---------------------------------------------------------------------------
# GraphQL payload shapes (typed subsets)
# ---------------------------------------------------------------------------


class ActorRef(msgspec.Struct, omit_defaults=True):
    """Author/actor subset used for bot and identity classification."""

    typename: str | None = msgspec.field(name="__typename", default=None)
    login: str | None = None


class PageInfo(msgspec.Struct, omit_defaults=True):
    """Page info shape for GraphQL pagination."""

    hasNextPage: bool
    endCursor: str | None = None


class OverviewRepository(msgspec.Struct, omit_defaults=True):
    """Repo-level facts fetched once per collection run."""

    pushedAt: str | None = None
    hasIssuesEnabled: bool = True
    isArchived: bool = False


class OverviewData(msgspec.Struct, omit_defaults=True):
    repository: OverviewRepository | None = None


class CreatedAtNode(msgspec.Struct, omit_defaults=True):
    createdAt: str


class OpenItemsConnection(msgspec.Struct, omit_defaults=True):
    totalCount: int = 0
    nodes: list[CreatedAtNode] | None = None
    pageInfo: PageInfo | None = None


class OpenIssuesRepository(msgspec.Struct, omit_defaults=True):
    issues: OpenItemsConnection | None = None


class OpenIssuesData(msgspec.Struct, omit_defaults=True):
    repository: OpenIssuesRepository | None = None


class OpenPullsRepository(msgspec.Struct, omit_defaults=True):
    pullRequests: OpenItemsConnection | None = None


class OpenPullsData(msgspec.Struct, omit_defaults=True):
    repository: OpenPullsRepository | None = None


class CommentNode(msgspec.Struct, omit_defaults=True):
    createdAt: str
    authorAssociation: str | None = None
    author: ActorRef | None = None


class CommentsConnection(msgspec.Struct, omit_defaults=True):
    nodes: list[CommentNode] | None = None
    pageInfo: PageInfo | None = None


class IssueCohortNode(msgspec.Struct, omit_defaults=True):
    number: int
    createdAt: str
    authorAssociation: str | None = None
    author: ActorRef | None = None
    comments: CommentsConnection | None = None
    timelineItems: TimelineConnection | None = None


class IssueCohortConnection(msgspec.Struct, omit_defaults=True):
    nodes: list[IssueCohortNode] | None = None
    pageInfo: PageInfo | None = None


class IssueCohortRepository(msgspec.Struct, omit_defaults=True):
    issues: IssueCohortConnection | None = None


class IssueCohortData(msgspec.Struct, omit_defaults=True):
    repository: IssueCohortRepository | None = None


class IssueCommentsOnly(msgspec.Struct, omit_defaults=True):
    comments: CommentsConnection | None = None


class IssueByNumberRepository(msgspec.Struct, omit_defaults=True):
    issue: IssueCommentsOnly | None = None


class IssueByNumberData(msgspec.Struct, omit_defaults=True):
    repository: IssueByNumberRepository | None = None


class ReviewNode(msgspec.Struct, omit_defaults=True):
    submittedAt: str | None = None
    authorAssociation: str | None = None
    author: ActorRef | None = None


class ReviewsConnection(msgspec.Struct, omit_defaults=True):
    nodes: list[ReviewNode] | None = None
    pageInfo: PageInfo | None = None


class CloserRef(msgspec.Struct, omit_defaults=True):
    """What performed an automatic issue close (a pull request or commit)."""

    typename: str | None = msgspec.field(name="__typename", default=None)


class TimelineNode(msgspec.Struct, omit_defaults=True):
    """
    One CLOSED_EVENT / MERGED_EVENT timeline entry.

    Shared by both cohort queries with disjoint field use: the PR query
    requests ``actor`` (actor-proxy qualification), the issue query requests
    ``closer`` (code-driven-close qualification).
    """

    typename: str | None = msgspec.field(name="__typename", default=None)
    createdAt: str | None = None
    actor: ActorRef | None = None
    closer: CloserRef | None = None


class TimelineConnection(msgspec.Struct, omit_defaults=True):
    nodes: list[TimelineNode] | None = None
    pageInfo: PageInfo | None = None


class PullCohortNode(msgspec.Struct, omit_defaults=True):
    number: int
    createdAt: str
    isDraft: bool = False
    authorAssociation: str | None = None
    author: ActorRef | None = None
    reviews: ReviewsConnection | None = None
    timelineItems: TimelineConnection | None = None


class PullCohortConnection(msgspec.Struct, omit_defaults=True):
    nodes: list[PullCohortNode] | None = None
    pageInfo: PageInfo | None = None


class PullCohortRepository(msgspec.Struct, omit_defaults=True):
    pullRequests: PullCohortConnection | None = None


class PullCohortData(msgspec.Struct, omit_defaults=True):
    repository: PullCohortRepository | None = None


class PullReviewsOnly(msgspec.Struct, omit_defaults=True):
    reviews: ReviewsConnection | None = None


class PullByNumberRepository(msgspec.Struct, omit_defaults=True):
    pullRequest: PullReviewsOnly | None = None


class PullByNumberData(msgspec.Struct, omit_defaults=True):
    repository: PullByNumberRepository | None = None


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

OVERVIEW_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    pushedAt
    hasIssuesEnabled
    isArchived
  }
}
""".strip()

OPEN_ISSUES_QUERY = (
    """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    issues(states: OPEN, first: %(open_page)d, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
      totalCount
      nodes { createdAt }
      pageInfo { hasNextPage endCursor }
    }
  }
}
""".strip()
    % PAGE_SIZES
)

OPEN_PULLS_QUERY = (
    """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: OPEN, first: %(open_page)d, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
      totalCount
      nodes { createdAt }
      pageInfo { hasNextPage endCursor }
    }
  }
}
""".strip()
    % PAGE_SIZES
)

ISSUE_COHORT_QUERY = (
    """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    issues(first: %(cohort_page)d, after: $after, orderBy: {field: CREATED_AT, direction: DESC}) {
      nodes {
        number
        createdAt
        authorAssociation
        author { __typename login }
        comments(first: %(comments_first)d) {
          nodes { createdAt authorAssociation author { __typename login } }
          pageInfo { hasNextPage endCursor }
        }
        timelineItems(itemTypes: [CLOSED_EVENT], first: %(timeline)d) {
          nodes {
            __typename
            ... on ClosedEvent { createdAt closer { __typename } }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
""".strip()
    % PAGE_SIZES
)

ISSUE_COMMENTS_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      comments(first: %(continuation)d, after: $after) {
        nodes { createdAt authorAssociation author { __typename login } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
""".strip()
    % PAGE_SIZES
)

PULL_COHORT_QUERY = (
    """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: %(cohort_page)d, after: $after, orderBy: {field: CREATED_AT, direction: DESC}) {
      nodes {
        number
        createdAt
        isDraft
        authorAssociation
        author { __typename login }
        reviews(first: %(reviews_first)d) {
          nodes { submittedAt authorAssociation author { __typename login } }
          pageInfo { hasNextPage endCursor }
        }
        timelineItems(itemTypes: [CLOSED_EVENT, MERGED_EVENT], first: %(timeline)d) {
          nodes {
            __typename
            ... on ClosedEvent { createdAt actor { __typename login } }
            ... on MergedEvent { createdAt actor { __typename login } }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
""".strip()
    % PAGE_SIZES
)

PULL_REVIEWS_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: %(continuation)d, after: $after) {
        nodes { submittedAt authorAssociation author { __typename login } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
""".strip()
    % PAGE_SIZES
)
