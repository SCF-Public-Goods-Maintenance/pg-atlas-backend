# source this script to set PG_ATLAS_MAINTENANCE_METRIC_ALLOWLIST in your terminal session
# run it if you only want the printed output to manually apply it in a different environment
# works when sourced from bash or zsh: the sourced path is captured at file
# scope, where zsh's $0 still carries it (inside a function it would not)

_pg_atlas_maintenance_helper_source="${BASH_SOURCE[0]:-$0}"

set_maintenance_allowlist_to_pg_proposals() {
  local projects_yaml
  projects_yaml="$(cd -- "$(dirname -- "${_pg_atlas_maintenance_helper_source}")/.." && pwd)/pg_atlas/data/projects/pg-proposal-overrides.yml"

  if [ ! -r "${projects_yaml}" ]; then
    printf 'set_maintenance_allowlist: cannot read %s\n' "${projects_yaml}" >&2
    unset -f set_maintenance_allowlist_to_pg_proposals
    unset _pg_atlas_maintenance_helper_source
    return 1
  fi

  local gh_repo_pairs
  gh_repo_pairs="$({
    awk '
      /git_repo_urls:/ { in_repo=1; next }
      in_repo && /- "https:\/\/github\.com\// {
        gsub(/^[[:space:]]*-[[:space:]]*"/, "")
        gsub(/"$/, "")
        sub(/^https:\/\/github\.com\//, "")
        vals = vals (vals ? "," : "") $0
        next
      }
      in_repo && /^[^[:space:]]/ { in_repo=0 }
      END { if (vals != "") print vals }
    ' "${projects_yaml}"
  })"

  export PG_ATLAS_MAINTENANCE_METRIC_ENABLED=true
  export PG_ATLAS_MAINTENANCE_METRIC_ALLOWLIST="${gh_repo_pairs}"

  printf 'PG_ATLAS_MAINTENANCE_METRIC_ALLOWLIST=%s\n' "${gh_repo_pairs}"

  local top_level_keys
  top_level_keys="$({
    awk '
      /^[^[:space:]]/ {
        key = $0
        gsub(/[[:space:]]*$/, "", key)
        if (key ~ /^#/) next
        if (key !~ /:$/) next
        sub(/:$/, "", key)
        if (key == "$schema") next
        print key
      }
    ' "${projects_yaml}" | paste -sd' ' -
  })"

  printf '\nRun the Reference Graph Bootstrap with:\n%s\n' "${top_level_keys}"

  unset -f set_maintenance_allowlist_to_pg_proposals
  unset _pg_atlas_maintenance_helper_source
}

set_maintenance_allowlist_to_pg_proposals
