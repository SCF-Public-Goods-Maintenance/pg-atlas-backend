<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD041 -->
## Registries — Registry Crawl Queue
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `registry-crawl` | {registry_crawl_succeeded} | {registry_crawl_failed} | {registry_crawl_todo} | {registry_crawl_cancelled} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
- **Warnings**: {warning_count} | **Errors**: {error_count}
- **Unsupported ecosystems**: {unsupported_ecosystem_group_count} groups / {unsupported_ecosystem_purl_count} packages

### Unsupported Ecosystems

{unsupported_ecosystems}

<details open>
<summary>Errors ({error_count})</summary>

{error_detail}

</details>

<details>
<summary>Warnings ({warning_count})</summary>

{warning_detail}

</details>
