# healthcheck.py
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import os
import copy
from datetime import datetime, timedelta
import logging
import sqlite3
import json
import time
from pathlib import Path
from observatorio_ipa.utils import db
from observatorio_ipa.core.config import (
    AutoRunSettings,
    HEALTHCHECK_HEARTBEAT_FILE,
    LOGGER_NAME,
)

logger = logging.getLogger(LOGGER_NAME)


class HealthHandler(BaseHTTPRequestHandler):
    # settings must be set before server starts
    settings: AutoRunSettings

    @classmethod
    def set_settings(cls, settings: AutoRunSettings, *args, **kwargs) -> None:
        # settings must be set before server starts
        cls.settings = copy.deepcopy(settings)
        # create a heartbeat_file if it doesn't exist
        if settings.heartbeat.heartbeat_file:
            hb_file = Path(settings.heartbeat.heartbeat_file)
            if not hb_file.exists():
                hb_file.parent.mkdir(parents=True, exist_ok=True)
                tz = settings.timezone or "UTC"
                hb_file.write_text(db.datetime_to_iso(db.tz_now(tz=tz)))

    def do_GET(self):
        if self.path == "/healthz":
            status = self.check_health()
            code = 200 if status["healthy"] else 503
            self.send_response(code)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(status).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def check_health(self):
        settings = self.settings
        db_path = Path(settings.db.db_path).expanduser().resolve()
        now = db.tz_now()
        poll_minutes = settings.orchestration_job.interval_minutes
        poll_sec = poll_minutes * 60
        result = {"healthy": True, "checks": {}}

        # DB connectivity
        try:
            with db.db(db_path) as conn:
                conn.execute("SELECT 1")
            result["checks"]["db"] = True
        except Exception as e:
            result["checks"]["db"] = False
            result["healthy"] = False
            result["db_error"] = str(e)
            logger.error(f"Healthcheck failed. DB connectivity issue: {str(e)}")

        # Export task staleness
        # Disabled for now. Creating error when poll has been offline for valid reasons.
        # try:
        #     with db.db(db_path) as conn:
        #         row = conn.execute(
        #             """
        #             SELECT MIN(next_check_at) AS min_next
        #             FROM exports
        #             WHERE state IN ('RUNNING')
        #         """
        #         ).fetchone()
        #         staleness = 0
        #         if row and row["min_next"]:
        #             min_next = datetime.fromisoformat(row["min_next"])
        #             staleness = max(0, int((now - min_next).total_seconds()))

        #         if staleness > poll_sec * 3:
        #             result["checks"]["stale_exports"] = False
        #             result["healthy"] = False
        #             logger.error(
        #                 f"Healthcheck failed: export tasks stale by {staleness}s (> {poll_sec * 3}s)"
        #             )
        #         else:
        #             result["checks"]["stale_exports"] = True
        # except Exception as e:
        #     result["checks"]["stale_exports"] = False
        #     result["healthy"] = False
        #     result["stale_error"] = str(e)
        #     logger.error(f"Healthcheck failed: export staleness check error: {str(e)}")

        # Heartbeat file recency

        try:
            hb = Path(settings.heartbeat.heartbeat_file)
            txt = hb.read_text(encoding="utf-8").strip()
            last_poll = datetime.fromisoformat(txt)
            age = int((now - last_poll).total_seconds())
            if age > (poll_sec * 3):
                logger.error(
                    f"Healthcheck failed: last successful poll {age}s ago (> {poll_sec * 3}s)"
                )
                logger.error(f"Heartbeat file path: {hb.as_posix()}")
                logger.error(f"Heartbeat last poll time: {last_poll.isoformat()}")
                logger.error(f"Current time: {now.isoformat()}")
                result["checks"]["heartbeat"] = False
                result["healthy"] = False
                result["_error"] = "Missing heartbeat file"
            else:
                result["checks"]["heartbeat"] = True
        except Exception as e:
            result["checks"]["heartbeat_recent"] = False
            result["healthy"] = False
            result["heartbeat_error"] = str(e)
            logger.error(f"Healthcheck failed: heartbeat file error: {str(e)}")

        return result


def start_healthcheck_server(settings: AutoRunSettings, port: int = 8080) -> HTTPServer:
    HealthHandler.set_settings(settings)
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
