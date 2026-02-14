#!/bin/bash
# Script to comment on all open Dependabot PRs to trigger rebase
# This resolves the TestContainers/Docker API incompatibility issue
# documented in .github/workflows/INVESTIGATION-SUMMARY.md

set -e

REPO_OWNER="osalvador"
REPO_NAME="ReplicaDB"
COMMENT_BODY="@dependabot rebase"

# List of open Dependabot PRs
DEPENDABOT_PRS=(
  222  # org.apache.commons:commons-csv from 1.7 to 1.14.1
  223  # com.oracle.database.xml:xmlparserv2 from 23.3.0.23.09 to 23.26.1.0.0
  224  # version.testContainers from 1.21.3 to 1.21.4 (contains the fix!)
  225  # com.google.code.gson:gson from 2.8.9 to 2.13.2
  226  # org.postgresql:postgresql from 42.7.2 to 42.7.9
  227  # actions/upload-artifact from 4 to 6
  228  # docker/setup-qemu-action from 1 to 3
  229  # docker/login-action from 1 to 3
  230  # rake requirement from ~> 10.0 to ~> 13.3 in /docs
  231  # docker/setup-buildx-action from 1 to 3
  232  # actions/setup-java from 4 to 5
  233  # jekyll requirement from ~> 3.3 to >= 3.3, < 5.0 in /docs
)

echo "=========================================="
echo "Rebasing Dependabot PRs in $REPO_OWNER/$REPO_NAME"
echo "=========================================="
echo ""

for PR_NUMBER in "${DEPENDABOT_PRS[@]}"; do
  echo "Commenting on PR #$PR_NUMBER..."
  
  if gh pr comment "$PR_NUMBER" --repo "$REPO_OWNER/$REPO_NAME" --body "$COMMENT_BODY"; then
    echo "✅ Successfully commented on PR #$PR_NUMBER"
  else
    echo "❌ Failed to comment on PR #$PR_NUMBER"
  fi
  
  # Small delay to avoid rate limiting
  sleep 1
done

echo ""
echo "=========================================="
echo "Done! All Dependabot PRs have been notified to rebase."
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Monitor CI runs - they should now pass with TestContainers 1.21.4"
echo "2. Auto-merge workflow will automatically merge PRs once CI passes"
echo "3. Check .github/workflows/INVESTIGATION-SUMMARY.md for details"
