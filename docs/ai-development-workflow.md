# AI-Assisted Development Workflow

This document describes how I use AI tooling while developing Zugzwang.

The goal is to use agents aggressively for implementation and research while keeping architectural decisions, validation, and commit history deliberate and understandable.

## Tool Responsibilities

### Antigravity IDE

Primary implementation environment.

Use Antigravity for:

* repository exploration
* research
* architecture proposals
* implementation
* refactoring
* tests
* Databricks configuration
* documentation
* local verification
* preparing commits

Antigravity may make substantial local changes before they are committed.

### Independent Reviewer

Use a separate model as an independent challenger when a decision or implementation deserves another opinion.

The reviewer should challenge:

* assumptions
* architecture
* technical claims
* source semantics
* unnecessary complexity
* Databricks usage
* implementation correctness
* scope creep
* whether something would withstand senior-level technical discussion

The reviewer should not simply continue Antigravity's reasoning.

---

# Normal Development Flow

## 1. Design

For non-trivial new functionality:

```text
/design <goal>
```

Use **high reasoning effort** for:

* architecture
* data modelling
* source evaluation
* ambiguous data semantics
* consequential Databricks decisions
* ADR-worthy decisions

A good `/design` request should normally be short because project rules, skills, ADRs, and existing architecture already provide context.

Example:

```text
/design Add reproducible source acquisition for the June 2026 vertical slice.
```

The design workflow should:

* read applicable rules
* use relevant skills
* respect existing ADRs
* inspect the current repository
* propose the smallest useful design
* explicitly state what should be deferred
* avoid reopening accepted decisions without new evidence

Do not immediately implement a design merely because the agent proposed it.

---

## 2. Challenge Important Decisions

Use independent review when:

* selecting data sources
* accepting a new architecture
* making a consequential modelling decision
* an answer appears overly confident
* Databricks functionality may have been added artificially
* source semantics are uncertain
* the proposed solution seems overengineered

For long Antigravity sessions, run:

```text
/handoff
```

and give the resulting handoff artifact to the independent reviewer.

A handoff should distinguish:

* evidence
* assumptions
* decisions
* implementation
* unresolved questions

Research hypotheses are allowed during exploration.

Committed documentation must contain verified facts.

---

## 3. Implement

Once a design is accepted:

```text
Implement the approved design.
Do not reopen the architecture unless implementation reveals a blocking issue.
```

Normally use **medium reasoning effort** for implementation.

Use high effort only when implementation reveals:

* difficult correctness issues
* ambiguous source behavior
* complex Spark behaviour
* architectural conflicts
* difficult debugging

Allow Antigravity to make coherent changes across multiple files.

Do not force one AI task to equal one Git commit.

---

## 4. Verify

Before considering a feature complete:

```text
/verify
```

Verification should run only checks that actually apply, such as:

```text
uv run pytest
uv run ruff check .
databricks bundle validate -t dev
```

For data work, verification should also consider:

* dataset grain
* key uniqueness
* row multiplication
* join coverage
* null behavior
* representative source records
* measured rather than assumed data-quality properties

Never claim something was verified unless the corresponding check actually ran.

---

## 5. Review

After implementation:

```text
/review
```

Antigravity should review the current diff as a senior engineer.

Prioritize:

1. correctness
2. source/data semantics
3. architecture
4. unnecessary complexity
5. Databricks-specific issues
6. tests
7. maintainability

Ignore cosmetic style issues unless they have practical consequences.

For important changes, optionally follow this with:

```text
/handoff
```

and ask for independent review.

---

# Git and Commit Workflow

Antigravity may make multiple related changes in the working tree before commits are created.

Do not automatically commit after each AI task.

When a logical milestone is complete:

```text
/prepare-commits
```

The workflow should:

1. inspect the complete working tree;
2. identify logical change groups;
3. propose commit boundaries;
4. keep implementation and directly related tests together;
5. use partial/hunk staging when one file contains changes belonging to different commits;
6. avoid one-commit-per-file grouping;
7. avoid mixing unrelated refactoring and features;
8. ensure commits leave the repository in a meaningful state.

Before creating commits, show the proposed plan.

Example:

```text
1. feat: add DWD source normalization
   - weather transformation
   - associated tests

2. feat: add station-weather mapping
   - spatial transformation
   - mapping tests

3. chore: configure Lakeflow pipeline resource
   - databricks.yml
   - pipeline definitions
```

After approval, create the commits in dependency order.

After committing:

* show the new commit history
* show remaining local changes
* report whether the working tree is clean

Never:

* discard user changes
* amend commits without being asked
* rebase published history without being asked
* force-push
* commit credentials or generated datasets

---

# How to Use Agent Configuration

## Rules

Rules encode behavior that should apply repeatedly without being mentioned in prompts.

Examples:

* project scope
* Python conventions
* evidence requirements
* credential handling
* architectural principles

Do not repeat these rules in every chat message.

## Skills

Skills contain specialized knowledge or procedures.

Examples:

* railway-data research
* Databricks development
* architecture review

Skills should normally activate automatically from the task context.

Explicitly request a skill only when automatic selection appears insufficient.

## Workflows

Workflows describe repeatable processes and should be invoked explicitly.

Primary workflows:

```text
/design
/verify
/review
/handoff
/prepare-commits
```

## Roles

Roles are optional perspectives such as architect, engineer, and reviewer.

Do not invoke roles merely because they exist.

Prefer workflows when a workflow already captures the desired behavior.

---

# Reasoning Effort

Recommended default:

```text
Architecture / research / source semantics    HIGH
Implementation                               MEDIUM
Tests / straightforward refactoring          MEDIUM
Verification                                 MEDIUM
Complex debugging                            HIGH
Consequential review                         HIGH
```

Higher reasoning effort does not replace evidence or verification.

---

# General Principle

Use the AI environment so that prompts can become shorter over time.

Instead of repeatedly writing detailed instructions, improve:

* rules for persistent constraints;
* skills for reusable expertise;
* workflows for repeated procedures;
* ADRs for accepted architectural decisions.

A healthy workflow should increasingly look like:

```text
/design feature
      ↓
challenge if consequential
      ↓
implement
      ↓
/verify
      ↓
/review
      ↓
/prepare-commits
      ↓
commit
```

The repository, ADRs, rules, and tests are the durable project memory.

Chat history is not.
