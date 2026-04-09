<!-- markdownlint-disable MD041 -->
## SBOM Queue Processing
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `sbom` | {sbom_succeeded} | {sbom_failed} | {sbom_todo} | {sbom_cancelled} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
- **Warnings**: {warning_count} | **Errors**: {error_count}

{error_detail}

### Parsed SPDX Documents

{spdx_details}
