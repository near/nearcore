---
description: Create a PR from staged changes with auto-generated branch, title, and description
---

Create a pull request with automatically generated title and description based on staged changes.

**User instructions:** $ARGUMENTS

If user instructions are non-empty, parse them for the following hints (they are free-form text, not structured flags):
- **"draft"** → create the PR as a draft
- **"fork"** → push to the user's own fork instead of `origin`
- **"reuse-branch"** → reuse the current branch instead of creating a new one
- **"new-branch"** → create a new branch even if on a feature branch
- **A base branch** (e.g., "base: shreyan/project/pr1", "base is master") → use as the PR base branch instead of auto-detecting
- **A project name** (e.g., "project is spice", "project: resharding") → use as the project instead of auto-detecting
- **A change type** (e.g., "type: fix", "type is refactor") → use as the change type instead of auto-detecting
- **Any other text** → treat as extra context to incorporate into the PR description

Follow these steps in order:

1. **Validate:**
   - Record the current branch name
   - Run `git diff --cached` to check for staged changes. If empty, warn the user and stop
   - Do NOT analyze unstaged changes
   - Fetch the GitHub username by running: `gh api user --jq .login`
   - **Determine `reuse-branch` and base branch:**
     - If user instructions specify "reuse-branch", set `reuse-branch = true`.
     - If user instructions specify "new-branch", set `reuse-branch = false`.
     - If neither is specified, auto-detect: if the current branch is NOT `master` and no base branch was specified in instructions, set `reuse-branch = true`; otherwise set `reuse-branch = false`.
     - **Base branch:** if user instructions specify a base branch, use that. Otherwise, use `master`.
     - When `reuse-branch` is true, use the current branch name as the branch name.
   - **Detect push remote:**
     - If user instructions contain "fork", determine the user's fork remote:
       1. List remotes: `git remote -v`
       2. Look for a remote whose URL contains the GitHub username (e.g., `github.com/{username}/`). This is the fork remote.
       3. Record the fork remote name for later use.
     - If "fork" is not specified, the push remote is `origin`.

2. **Analyze staged changes:**
   - Run `git diff --cached` to get the full staged diff
   - Read the modified files to understand the surrounding code context
   - Identify the project/area (e.g., spice, resharding, state-sync, ci). If changes span multiple projects, pick the most dominant one. If user instructions specify a project, use that instead.
   - Read the `CONTRIBUTING.md` "## Pull Requests" section for valid change types and PR title conventions
   - Determine the change type (fix, feat, refactor, doc, test, chore, perf, revert). If multiple apply, pick the most dominant. If user instructions specify a type, use that instead.
   - Generate a short hyphenated task name, 2-4 words (e.g., `add-metrics`, `fix-header-validation`, `refactor-chunk-apply`)

3. **Generate all details — do NOT run any git commands in this step:**
   - **Branch name (local)** and **remote branch name:**
     - If `reuse-branch` is true: the local branch name is the current branch. Skip collision checks.
     - Otherwise, generate a new local branch name:
       - If pushing to a fork: `{project}/{task}`
       - If pushing to origin: `{username}/{project}/{task}`
       - Check for collisions: `git branch --list <name>` (local) and `git ls-remote --heads <push-remote> <name>` (remote)
       - On collision, append `-2`, `-3`, etc.
     - **Remote branch name:** when pushing to origin, the remote branch must have the `{username}/` prefix. If the local branch name already starts with `{username}/`, the remote name is the same. Otherwise, prepend it: `{username}/{local-branch-name}`. When pushing to a fork, the remote branch name is the same as the local branch name.
   - **Commit message:** single line, same style as the PR title
   - **PR title:** `<type>(<project>): <title>` — title must be lowercase (per `CONTRIBUTING.md`)
     - Example: `feat(state-sync): add metrics tracking`
   - **PR description:**
     - Concise summary of what changed and why. Use prose, bullet points, or a mix — whatever fits the change best. A one-line description is fine for small changes.
     - Incorporate any extra context from user instructions
     - Do not include implementation details, test plan sections, or AI attribution
   - **Draft:** true if user instructions contain "draft", false otherwise
   - **Push remote:** the fork remote name if "fork" was specified, otherwise `origin`

4. **Confirm with user:**
   - First, print the details in exactly this format (no horizontal rules, no extra fields):
     ```
     Branch: <local-branch-name> (remote: <remote-branch-name>)
     Push remote: <push-remote>
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
   - If `reuse-branch` is false, create the branch: `git checkout -b <branch-name>`
   - Commit staged changes: `git commit -m "<commit message>"`
   - Push: `git push -u <push-remote> <local-branch-name>:<remote-branch-name>`
     (If local and remote names are the same, `git push -u <push-remote> <branch-name>` is fine.)
   - Create the PR using a HEREDOC for the body:
     - If pushing to a fork, use `--head <username>:<remote-branch-name>`:
       ```
       gh pr create --base <base-branch> --head <username>:<remote-branch-name> \
         --title "<title>" \
         --body "$(cat <<'EOF'
       <description>
       EOF
       )"
       ```
     - If pushing to origin, use `--head <remote-branch-name>`:
       ```
       gh pr create --base <base-branch> --head <remote-branch-name> \
         --title "<title>" \
         --body "$(cat <<'EOF'
       <description>
       EOF
       )"
       ```
     Add `--draft` if the draft flag is set.
   - Display the PR URL on success
   - If push or PR creation fails but the branch was already created locally, tell the user the branch exists and suggest how to retry
