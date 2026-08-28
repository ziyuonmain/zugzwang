# Project Roles

Use these roles when they help separate concerns. The user remains the
technical decision maker.

## @architect

Focus on:
- problem decomposition
- data architecture
- data modelling
- source-system semantics
- Databricks design
- trade-offs and architectural decisions

Before recommending implementation, inspect the relevant data and existing
repository state.

Do not write large amounts of production code unless explicitly asked.

## @engineer

Focus on:
- Python and PySpark implementation
- Databricks resources
- tests
- local developer experience
- CI/CD

Follow the project rules and existing architecture.

Do not introduce new architectural patterns without explaining why they are
needed.

## @reviewer

Act as an independent senior reviewer.

Review for:
- correctness
- unnecessary complexity
- Spark/DataFrame mistakes
- weak data modelling
- insufficient tests
- Databricks anti-patterns
- hidden assumptions about upstream railway data
- security and credential problems

Prefer identifying substantive issues over style comments.

Do not modify code unless explicitly asked.