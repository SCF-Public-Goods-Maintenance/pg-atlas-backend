<!-- markdownlint-disable MD041 -->
## deps.dev — Package Dependencies Queue
<!-- markdownlint-enable MD041 -->

| Queue | Succeeded | Failed | Pending | Cancelled |
| ----- | --------: | -----: | ------: | --------: |
| `package-deps` | {package_deps_succeeded} | {package_deps_failed} | {package_deps_todo} | {package_deps_cancelled} |

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
