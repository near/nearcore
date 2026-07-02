#!/usr/bin/env bash
set -euo pipefail

# Fetches PR comments via GraphQL and formats them for Claude review.
#
# Required env vars: GH_TOKEN, PR_NUMBER, REPO_OWNER, REPO_NAME
# Outputs: /tmp/pr_comments_context.txt

QUERY='query($owner: String!, $repo: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      comments(first: 100) {
        totalCount
        nodes {
          author { login }
          body
          createdAt
        }
      }
      reviewThreads(first: 100) {
        totalCount
        nodes {
          isResolved
          isOutdated
          path
          line
          comments(first: 50) {
            nodes {
              author { login }
              body
              createdAt
              diffHunk
            }
          }
        }
      }
      reviews(first: 50) {
        totalCount
        nodes {
          author { login }
          body
          state
          createdAt
        }
      }
    }
  }
}'

# Execute GraphQL query and check for errors
if ! COMMENTS_JSON=$(gh api graphql \
  -f query="$QUERY" \
  -f owner="$REPO_OWNER" \
  -f repo="$REPO_NAME" \
  -F prNumber="$PR_NUMBER"); then
  echo "Warning: Failed to fetch PR comments. Proceeding without comment context."
  echo "⚠️ Unable to fetch existing comments due to API error." > /tmp/pr_comments_context.txt
  exit 0
fi

# Project the relevant fields to JSON context for Claude, truncating long diff
# hunks to keep the token budget bounded. The LLM reads JSON directly, so no
# prose formatting is needed.
# Write to file instead of env var to avoid E2BIG on large PRs
if [ -n "$COMMENTS_JSON" ]; then
  echo "$COMMENTS_JSON" > /tmp/pr_comments_json.txt
  if ! jq '
      # Truncate a diff hunk to ~$max chars at line boundaries (never mid-line).
      def truncate_hunk($max):
        if (length <= $max) then .
        else
          (reduce (split("\n")[]) as $line ({acc: [], count: 0, done: false};
             if .done then .
             elif ((.count + ($line | length) + 1) > $max and (.acc | length) > 0)
             then .done = true
             else {acc: (.acc + [$line]), count: (.count + ($line | length) + 1), done: false}
             end)
           | .acc | join("\n")) + "\n... (truncated)"
        end;
      (.data.repository.pullRequest // {}) | {
        comments: [(.comments.nodes // [])[] | {author: .author.login, body, createdAt}],
        reviews: [(.reviews.nodes // [])[] | select((.body // "") != "") | {author: .author.login, state, body, createdAt}],
        threads: [(.reviewThreads.nodes // [])[] | {path, line, isResolved, isOutdated,
          comments: [(.comments.nodes // [])[] | {author: .author.login, body, createdAt, diffHunk: ((.diffHunk // "") | truncate_hunk(500))}]}]
      }' /tmp/pr_comments_json.txt > /tmp/pr_comments_context.txt; then
    echo "⚠️ Unable to parse comment data." > /tmp/pr_comments_context.txt
  fi
else
  echo "⚠️ No comments data to process." > /tmp/pr_comments_context.txt
  exit 0
fi
