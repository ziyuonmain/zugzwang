---
description: description: Split the current working tree into meaningful commits
---

Inspect the full current git diff and repository status.

Do not modify implementation unless needed to resolve an obvious commit-boundary issue.

Goal:
Turn the current local changes into a small set of coherent, reviewable commits.

Process:

1. Inspect:
   - `git status`
   - staged changes
   - unstaged changes
   - untracked files
   - relevant recent commits for message style

2. Group changes by logical intent, not by file type.

Good commit boundaries:
- one architectural/configuration change
- one implementation capability
- its directly related tests
- documentation that belongs to that same change

Avoid:
- one commit per file
- mixing unrelated refactors with feature work
- separating tests from the implementation they validate without a reason
- commits that leave the repository knowingly broken

3. Propose the commit plan before committing.

For each proposed commit provide:
- commit message
- purpose
- files or hunks included
- dependencies on earlier commits

4. Check whether individual files contain changes belonging to multiple commits.
If so, use partial staging/hunk staging rather than forcing the whole file
into one commit.

5. After approval, stage and commit each group in order.

6. After every commit:
- show the resulting commit SHA and message
- verify the remaining working tree
- run relevant lightweight checks when needed

7. At the end:
- show `git log --oneline` for the new commits
- show remaining uncommitted changes
- state whether the working tree is clean

Never:
- discard user changes
- amend existing commits unless explicitly requested
- force-push
- rebase published history
- include secrets or generated data