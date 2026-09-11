<!-- markdownlint-disable MD041 -->
## Criticality Materialization
<!-- markdownlint-enable MD041 -->

| Dep nodes seen | Active nodes scored | Duration (s) |
| -------------: | ------------------: | -----------: |
| {criticality_dep_nodes_seen} | {criticality_active_nodes_scored} | {criticality_duration_seconds} |

## Adoption Materialization

| Repos seen | Repo composites computed | Projects seen | Projects scored | Duration (s) |
| ---------: | -----------------------: | ------------: | --------------: | -----------: |
| {adoption_repos_seen} | {adoption_repo_composites_computed} | {adoption_projects_seen} | {adoption_projects_scored} | {adoption_duration_seconds} |

## Maintenance Materialization

| Gate skipped | Repos eligible | Profiles written | Stale profiles cleared | Duration (s) |
| :----------- | -------------: | ---------------: | ---------------------: | -----------: |
| {maintenance_gate_skipped} | {maintenance_repos_eligible} | {maintenance_profiles_written} | {maintenance_stale_profiles_cleared} | {maintenance_duration_seconds} |

- **Trigger**: {trigger}
- **Run**: [{run_id}]({server_url}/{repository}/actions/runs/{run_id})
- **Finished**: {finished}
