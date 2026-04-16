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

### Criticality Materialization

| Dep nodes seen | Active nodes scored | Duration (s) |
| -------------: | ------------------: | -----------: |
| {criticality_dep_nodes_seen} | {criticality_active_nodes_scored} | {criticality_duration_seconds} |
