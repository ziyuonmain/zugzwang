---
name: architecture-review
description: Review proposed Zugzwang architecture and implementation choices for simplicity, production realism, Databricks fit, maintainability, and unnecessary technology. Use before major architecture decisions or after completing a project milestone.
---

# Architecture review

Review the proposed solution as a senior data/platform engineer.

Evaluate:

## Problem fit
- What concrete problem does this component solve?
- Is it needed in the current project scope?

## Databricks fit
- Is Databricks being used because it provides real value?
- Could a simpler component do the job better?
- Are Spark and Delta used appropriately?

## Data design
- Are source semantics preserved?
- Are keys and grain explicit?
- Can records be traced back to their source?
- Is incremental processing correct?

## Software engineering
- Is important logic testable outside notebooks?
- Are responsibilities clear?
- Is configuration separate from code?

## Operations
- How is the component deployed?
- How does it fail?
- Can it be rerun safely?
- Is observability sufficient for the project's scope?

## Scope
Explicitly flag:
- premature abstraction
- resume-driven architecture
- needless enterprise patterns
- unnecessary Databricks services

Conclude with:
- Keep
- Change
- Defer