# Phase 10: baseline host-test CI

## Scope

The first Phase 10 slice adds a GitHub Actions workflow at
`.github/workflows/ci.yml`. It runs the repository's default Python test suite
on pull requests and on pushes to `main`. This is an automated check for
host-side code, not deployment or full-pipeline verification.

The workflow uses an Ubuntu runner and a Python 3.10 / 3.13 matrix to check
both ends of the declared Python range. Each job checks out the repository,
installs the `dev`, `ml`, `serving`, and `dashboard` extras, runs `pip check`,
and then runs `python -m pytest -q`. It grants the workflow token read-only
repository contents access and does not retain checkout credentials.

## Local equivalent

From the repository root, with a supported Python virtual environment active:

```powershell
python -m pip install --editable ".[dev,ml,serving,dashboard]"
python -m pip check
$env:RUN_POSTGRES_INTEGRATION = "0"
python -m pytest -q
```

`RUN_POSTGRES_INTEGRATION=0` makes the optional PostgreSQL integration test
skip. The CI jobs do not start Docker, PostgreSQL, Kafka, Spark, Airflow, or
an API server, and they do not use the raw Synerise data or local MLflow
artifacts. The API and dashboard tests use test doubles for those external
dependencies.

## Verification

Before the workflow was committed, the Windows host suite reported
**83 passed, 1 skipped**. On pull request #14, both the Python 3.10 and 3.13
CI jobs were reported green before merge. The workflow was then merged into
`main`. This establishes the first cross-version host-test check; it does
not imply that every optional integration or deployment path has passed.

For subsequent changes, inspect both `host-tests` matrix results in the PR's
Checks tab. A failing job should be investigated before merge. A green job
means only that its install, dependency check, and host tests completed on
that runner. This workflow does not itself configure branch-protection rules
or make the check required for merging.

## Current gaps

- The PostgreSQL rollback/resume integration test remains opt-in and needs a
  prepared local database and bronze event files.
- Spark streaming, dbt builds, Airflow DAG execution, and end-to-end replay
  are not exercised by this workflow.
- Local MLflow candidate-model artifacts are not available to CI, so model
  registry loading is tested with fakes rather than a real registry.
- No image build, deployment, observability check, security scan, or
  production-release process is defined here.

These are separate future checks; do not treat the baseline workflow as a
production readiness signal.
