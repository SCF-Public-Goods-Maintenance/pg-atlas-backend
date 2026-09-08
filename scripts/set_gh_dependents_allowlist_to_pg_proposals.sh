# source this script to set PG_ATLAS_GITHUB_DEPENDENTS_ALLOWLIST in your terminal session
# run it if you only want the printed output to manually apply it in a different environment

set_gh_dependents_allowlist_to_pg_proposals() {
  local projects_yaml
  projects_yaml="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/pg_atlas/data/projects/pg-proposal-overrides.yml"

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

  export PG_ATLAS_GITHUB_DEPENDENTS_ENABLED=true
  export PG_ATLAS_GITHUB_DEPENDENTS_ALLOWLIST="${gh_repo_pairs}"

  printf 'PG_ATLAS_GITHUB_DEPENDENTS_ALLOWLIST=%s\n' "${gh_repo_pairs}"

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

  unset -f set_gh_dependents_allowlist_to_pg_proposals
}

set_gh_dependents_allowlist_to_pg_proposals
