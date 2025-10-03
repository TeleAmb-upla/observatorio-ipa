#!/bin/bash
set -e

# Apply migrations
python src/observatorio_ipa/web/manage.py migrate

# Start Gunicorn
exec gunicorn webapp.wsgi:application --bind 0.0.0.0:8000 --chdir src/observatorio_ipa/web
