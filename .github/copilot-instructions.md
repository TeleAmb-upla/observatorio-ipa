# Copilot Instructions for observatorio-ipa

## Project Overview

- Automates satellite image processing for snow cover monitoring in Chile (OSN project).
- Main components: Python automation scripts, Django web interface, database (Postgres), and Google Earth Engine integration.
- Code is organized under `src/observatorio_ipa/` (core logic, services, web), with tests in `tests/`.
- Project is set to run in two docker containers: one for the main automation and another for the web interface.
- Project dependencies is managed with uv.

## Architecture & Data Flow

- **Core automation**: Entry point is `src/observatorio_ipa/core/scheduler.py`, which loads config, sets up logging, and runs two scheduled processes a job initiator and a job orchestrator.
- **Web interface**: Django app in `src/observatorio_ipa/web/webapp/` (settings, URLs, templates, manage.py). Uses `django-allauth` for accounts and `debug_toolbar` for debugging.
- **Database**: Uses Postgres, configured via Docker Compose (`db.docker-compose.yaml`). Secrets are stored in `secrets/` and injected at runtime.
- **Config**: Centralized in `core/config.py` (Pydantic models, TOML/env loading, validation).
- **Messaging**: Email notifications via `utils/messaging.py` and `services/messaging/email.py`.
- **Google Earth Engine**: Integrated via `services/gee/` and `gee-toolbox` dependency.

## Developer Workflows

- **Build/Run**: Use Docker for environment setup. Main automation runs via `scheduler.py`. Web interface managed with Django (`manage.py`).
- **Testing**: Tests are in `tests/` (organized by domain). Use `pytest` for running tests.
- **Config**: Environment variables and TOML files in `configs/`. Secrets in `secrets/` (never commit real secrets).
- **Database**: Start DB with `docker-compose -f db.docker-compose.yaml up`. Data persists in `db/`.

## Project-Specific Patterns

- All configuration and default settings are centralized in `core/config.py`.
- The core automation process uses sqlalchemy ORM models defined in `core/dbschema.py` for database interactions.
- Logging is always initialized via `utils/logs.py` and uses a named logger (`osn-ipa`).
- Email addresses are validated before sending (see `utils/messaging.py`).
- Django settings and URLs are split for modularity; jobs are routed via `jobs.urls`.
- Using `bootstrap5` for frontend styling in Django templates.

## Integration Points

- **Google Earth Engine**: API keys and service accounts in `secrets/`, used by `services/gee/`.
- **Database**: Credentials injected from secret files, not hardcoded.
- **Web**: Django templates in `web/templates/` or specific app templates in `web/templates/<app_name>/` or within a templates in the app itself `web/<app_name>/template/<app_name>/`, static files in `web/static/`.

## Examples

- To run main automation: `python -m observatorio_ipa.core.scheduler -t 'configs/test.toml'`
- To run web: `python src/observatorio_ipa/web/manage.py runserver`
- To run tests: `pytest tests/`
- To start DB: `docker-compose -f db.docker-compose.yaml up`

## Key Files & Directories

- `src/observatorio_ipa/core/scheduler.py`: Main automation entry
- `src/observatorio_ipa/web/webapp/`: Django web app
- `src/observatorio_ipa/core/config.py`: Central config
- `db.docker-compose.yaml`: Database setup
- `tests/`: Test suite
- `configs/`: Environment/config files
- `secrets/`: Credentials (never commit real secrets)

---

If any section is unclear or missing important project-specific details, please provide feedback to improve these instructions.
