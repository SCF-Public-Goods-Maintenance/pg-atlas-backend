<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD041 -->
## Crawl — OpenGrants Queue
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `opengrants` | {opengrants_succeeded} | {opengrants_failed} | {opengrants_todo} | {opengrants_cancelled} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
- **Warnings**: {warning_count} | **Errors**: {error_count}

<details open>
<summary>Errors ({error_count})</summary>

{error_detail}

</details>

<details>
<summary>Warnings ({warning_count})</summary>

{warning_detail}

</details>
