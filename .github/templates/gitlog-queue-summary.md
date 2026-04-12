<!-- markdownlint-disable MD041 -->
## Gitlog Queue Processing
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `gitlog` | {gitlog_succeeded} | {gitlog_failed} | {gitlog_todo} | {gitlog_cancelled} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
- **Warnings**: {warning_count} | **Errors**: {error_count}
- **First rate-limit hit after N repos**: {first_rate_limit_hit_after_n_repos}
- **Total rate-limit hits**: {total_rate_limit_hits}

{error_detail}

### gh auth status

```text
{gh_auth_status}
```

### Repos marked private (terminal git failures)

{terminal_failures_marked_private}
