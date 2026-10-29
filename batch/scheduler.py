"""Simple in-container scheduler for the batch reconciliation job.

* Once per day, after BATCH_DAILY_RUN_TIME (business timezone), runs the previous business date.
* Every BATCH_INTRADAY_INTERVAL_S seconds, runs the current business date ("so far") so control totals
  and quality checks stay fresh during the day. Set to 0 to disable.

In production this would be replaced by the organisation's scheduler (cron, Kubernetes CronJob,
Airflow, ...). The job itself is idempotent, so overlapping or repeated triggers are safe.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, date, datetime
from datetime import time as dtime

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from batch.daily_reconciliation import RunInProgressError, previous_business_date, run_business_date
from common.config import (
    BusinessSettings,
    DatabaseSettings,
    ReconciliationSettings,
    ServiceSettings,
    _Base,
    load,
)
from common.logging_setup import configure_logging
from common.signals import install_signal_handlers
from database.connection import connect
from observability import metrics
from processing.rules import load_rules

log = logging.getLogger("batch.scheduler")


class SchedulerSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="BATCH_", extra="ignore", case_sensitive=False, frozen=True)

    daily_run_time: dtime = dtime(0, 30)
    intraday_interval_s: int = Field(default=900, ge=0)


def main() -> None:
    service = load(ServiceSettings)
    configure_logging(service.service_name or "batch-scheduler", service.environment, service.log_level)
    sched, business = load(SchedulerSettings), load(BusinessSettings)
    db_settings = load(DatabaseSettings)
    rules = load_rules(load(ReconciliationSettings).rules_path)
    health = metrics.HealthState()
    if service.metrics_enabled:
        metrics.start_http_server(service.metrics_port, health)
    stop = threading.Event()
    install_signal_handlers(stop)
    health.set_ready(True, "scheduling")

    last_daily: date | None = None
    last_intraday = 0.0
    while not stop.is_set():
        now = datetime.now(UTC)
        local = now.astimezone(business.tz)
        jobs: list[tuple[date, str]] = []
        target = previous_business_date(business.tz, now)
        if local.time() >= sched.daily_run_time and last_daily != target:
            jobs.append((target, "DAILY"))
        if sched.intraday_interval_s and now.timestamp() - last_intraday >= sched.intraday_interval_s:
            jobs.append((local.date(), "INTRADAY"))
        for business_date, kind in jobs:
            try:
                with connect(db_settings) as conn:
                    summary = run_business_date(conn, rules, business.tz, business_date, "DAILY", f"scheduler:{kind}")
                log.info("scheduled run complete", extra={"business_date": str(business_date), "kind": kind,
                                                          "outcome": summary.status})
                if kind == "DAILY":
                    last_daily = business_date
                else:
                    last_intraday = now.timestamp()
            except RunInProgressError:
                log.info("run already in progress; skipping", extra={"business_date": str(business_date)})
            except Exception:  # noqa: BLE001 - retry on the next tick
                log.exception("scheduled run failed; will retry", extra={"business_date": str(business_date)})
        stop.wait(30)


if __name__ == "__main__":
    main()
