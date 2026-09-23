# Phase 0 — Project Setup

## Outcome

Phase 0 establishes a reproducible development foundation without introducing
any data-pipeline services.

The project now has:

- A Git repository connected to GitHub
- An isolated Python virtual environment
- Python project metadata
- Environment-variable conventions
- A minimal Docker image
- A Docker Compose smoke test

Kafka, Spark, PostgreSQL, dbt, Airflow, and ML components have not been added yet.

## Current architecture

```text
Developer
   |
   |-- Git repository
   |
   |-- Local Python virtual environment
   |
   `-- Docker Compose
            |
            `-- Temporary Python smoke-test container
```

There is no data flow in Phase 0. Docker Compose currently verifies only that a
reproducible Python container can be built and executed.

## Why these tools are used

### Git

Git records the project's history and makes changes reviewable and reversible.
The GitHub remote provides off-machine storage and a portfolio-facing repository.

### Python virtual environment

`.venv` isolates project packages from the system Python installation. This
prevents dependency changes in one project from affecting another project.

### Environment variables

Local configuration belongs in `.env`, which Git ignores. `.env.example`
documents expected variable names without exposing secrets.

### Docker

Docker packages an application with its runtime and operating-system
dependencies. It reduces differences between development environments.

### Docker Compose

Compose describes how containers are built, configured, and connected. It
currently manages one smoke-test container and will later coordinate the
platform's services.

## Files created in this phase

```text
.
|-- docker/
|   `-- Dockerfile
|-- docs/
|   `-- phase-0.md
|-- .dockerignore
|-- .env.example
|-- .gitignore
|-- compose.yaml
|-- pyproject.toml
`-- README.md
```

The `.venv` and `.env` files are local-only and deliberately excluded from Git.
Application source directories will be created when Phase 1 begins.

## Verification commands

Run these commands from the repository root:

```powershell
.\.venv\Scripts\Activate.ps1
python --version
python -m pip --version
python -c "import pathlib, tomllib; tomllib.loads(pathlib.Path('pyproject.toml').read_text()); print('pyproject.toml: valid')"
docker compose config --quiet
docker compose run --rm -T --interactive=false phase0-check
git check-ignore -v .env .venv
git status --short
```

Successful container output:

```text
Phase 0 container is ready
Python 3.13.x
```

The Compose command uses `-T` and `--interactive=false` because this smoke test
does not need a terminal or standard input. This ensures PowerShell receives its
prompt again as soon as the one-off container exits.

## Troubleshooting

### PowerShell blocks virtual-environment activation

Use the virtual environment's interpreter directly:

```powershell
.\.venv\Scripts\python.exe --version
```

### Docker cannot connect to the engine

Start Docker Desktop, wait for the engine to become ready, and run:

```powershell
docker info
```

Then retry the Compose command.

### The smoke test prints output but PowerShell does not return

First, run the smoke test without an interactive terminal:

```powershell
docker compose run --rm -T --interactive=false phase0-check
```

If other commands such as `docker ps` also stop responding, restart Docker
Desktop and wait for its engine to become ready. Verify recovery with
`docker info` before retrying the smoke test.

### A secret appears in Git status

Confirm that `.env` is ignored:

```powershell
git check-ignore -v .env
```

Never commit `.env`, passwords, API keys, or database credentials.

## Interview questions I should be able to answer

- Why do we use a Python virtual environment?
- What belongs in `.gitignore`?
- Why should `.env` not be committed?
- What is the difference between a Docker image and a container?
- What problem does Docker solve?
- What role does Docker Compose play?
