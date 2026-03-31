<!-- markdownlint-disable MD041 -->
## Reference Graph Bootstrap
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `opengrants` | {opengrants_succeeded} | {opengrants_failed} | {opengrants_todo} | {opengrants_cancelled} |
| `package-deps` | {package_deps_succeeded} | {package_deps_failed} | {package_deps_todo} | {package_deps_cancelled} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
- **Warnings**: {warning_count} | **Errors**: {error_count}

{error_detail}
