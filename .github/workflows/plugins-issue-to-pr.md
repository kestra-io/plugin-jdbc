---
# GitHub Actions triggers (same syntax you already know)
on:
  issues:
    types: [opened, labeled, reopened]
  pull_request:
    types: [opened, reopened, ready_for_review, labeled, synchronize]

# Pick your engine (set the matching secret in repo settings)
# engine: copilot | claude | codex
engine: copilot

permissions:
  contents: read
  issues: read
  pull-requests: read

safe-outputs:
  create-pull-request:
    title-prefix: "[plugins] "
    labels: [automation, plugins]
    draft: true
    if-no-changes: "warn"
    fallback-as-issue: false

  # IMPORTANT: gh-aw built-in add-reviewer restricts to GitHub *usernames* (not teams).
  # We'll handle @plugins via CODEOWNERS (recommended) or via a comment mention.
  add-comment:
    max: 1
---

# Plugins — Issue/PR → PR + Review routing

## Mandatory rules (Kestra)
You MUST follow Kestra’s agent rules for plugin development:
https://kestra.io/docs/plugin-developer-guide/use-agents

## Goal
- For every issue labeled `area/plugin`, create a draft PR implementing the request.
- For already-open PRs in the plugins area, ensure the plugins team is notified and review is routed correctly.

## Scope detection
Treat the item as plugins-scoped if:
- It has label `area/plugin` (exact match, case-insensitive), OR
- (PR-only) its title starts with `[plugins]`

If it does not match, do nothing and exit cleanly.

## Anti-duplicate guardrails (issue-triggered)
Before making changes, check whether a PR already exists for this issue (open or draft), using any of:
- PR body contains `#<issue_number>` or `Closes #<issue_number>` / `Fixes #<issue_number>`
- PR title contains the issue number
- branch name like `plugins/issue-<issue_number>-...`

If a PR already exists:
1) do not create a new PR
2) add a comment to the issue linking the existing PR
3) exit

## Work (issue-triggered, only if `area/plugin`)
1) Read the issue and extract expected behavior + acceptance criteria.
2) Implement small, focused changes following repo conventions.
3) Run relevant tests/linters (at least plugin-related subset if full suite is too long).
4) Create PR:
   - Branch: `plugins/issue-<issue_number>-<short-slug>`
   - Title: `[plugins] <short summary> (closes #<issue_number>)`
   - Body: context, changes, how to test, risks, and `Closes #<issue_number>`
5) Comment on the issue with the PR link + quick recap.

## Work (PR-triggered, already-open PRs)
When triggered by PR events:
- If plugins-scoped, add a comment mentioning @kestra-io/plugins with quick review instructions.
- Do not change code unless the PR explicitly asks for it.
