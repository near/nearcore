---
description: Create a PR from staged changes with auto-generated branch, title, and description
---

Create a pull request with automatically generated title and description based on staged changes.

**User instructions:** $ARGUMENTS

If user instructions are non-empty, parse them for the following hints (they are free-form text, not structured flags):
- **"draft"** → create the PR as a draft
- **A project name** (e.g., "project is spice", "project: resharding") → use as the project instead of auto-detecting
- **A change type** (e.g., "type: fix", "type is refactor") → use as the change type instead of auto-detecting
- **Any other text** → treat as extra context to incorporate into the PR description

Follow these steps in order:

1. **Validate:**
   - Record the current branch name — this is the PR base branch
   - Run `git diff --cached` to check for staged changes. If empty, warn the user and stop
   - Do NOT analyze unstaged changes
   - Fetch the GitHub username by running: `gh api user --jq .login`

2. **Analyze staged changes:**
   - Run `git diff --cached` to get the full staged diff
   - Read the modified files to understand the surrounding code context
   - Identify the project/area (e.g., spice, resharding, state-sync, ci). If changes span multiple projects, pick the most dominant one. If user instructions specify a project, use that instead.
   - Read the `CONTRIBUTING.md` "## Pull Requests" section for valid change types and PR title conventions
   - Determine the change type (fix, feat, refactor, doc, test, chore, perf, revert). If multiple apply, pick the most dominant. If user instructions specify a type, use that instead.
   - Generate a short hyphenated task name, 2-4 words (e.g., `add-metrics`, `fix-header-validation`, `refactor-chunk-apply`)

3. **Generate all details — do NOT run any git commands in this step:**
   - **Branch name:** `{username}/{project}/{task}`
     - Check for collisions: `git branch --list <name>` (local) and `git ls-remote --heads origin <name>` (remote)
     - On collision, append `-2`, `-3`, etc.
   - **Commit message:** single line, same style as the PR title
   - **PR title:** `<type>(<project>): <title>` — title must be lowercase (per `CONTRIBUTING.md`)
     - Example: `feat(state-sync): add metrics tracking`
   - **PR description:**
     - Concise summary of what changed and why. Use prose, bullet points, or a mix — whatever fits the change best. A one-line description is fine for small changes.
     - Incorporate any extra context from user instructions
     - Do not include implementation details, test plan sections, or AI attribution
   - **Draft:** true if user instructions contain "draft", false otherwise

4. **Confirm with user:**
   - First, print the details in exactly this format (no horizontal rules, no extra fields):
     ```
     Branch: <branch-name>
     Base branch: <base-branch>
     Draft: Yes/No

     PR title: <title>

     PR description:
     <description>
     ```
   - Then use `AskUserQuestion` with two options: "Yes, create PR" / "Abort"
     - Do NOT put the details inside the `AskUserQuestion` options
     - The user can select "Other" (built-in) to provide edits — apply their changes and re-confirm
   - If "Abort": stop immediately. Nothing was modified, so no cleanup is needed.

5. **Execute — only after user confirms:**
   - Create the branch: `git checkout -b <branch-name>`
   - Commit staged changes: `git commit -m "<commit message>"`
   - Push: `git push -u origin <branch-name>`
   - Create the PR using a HEREDOC for the body:
     ```
     gh pr create --base <base-branch> --head <branch-name> \
       --title "<title>" \
       --body "$(cat <<'EOF'
     <description>
     EOF
     )"
     ```
     Add `--draft` if the draft flag is set.
   - Display the PR URL on success
   - If push or PR creation fails but the branch was already created locally, tell the user the branch exists and suggest how to retry
