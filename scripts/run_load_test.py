"""Load test: publish a synthetic workload and measure the pipeline end to end.

Measures (all from real observations; nothing is estimated):
  * producer throughput (acknowledged events/s)
  * ingestion throughput (Bronze rows committed per second, first->last ingestion_time)
  * Kafka->Bronze latency percentiles (ingestion_time - Kafka record timestamp)
  * last event -> case evaluated latency percentiles (reconciliation freshness)
  * maximum consumer lag sampled during the test
  * reconciliation throughput (cases evaluated per second)
  * duration of a full business-date reconciliation run for the test window
  * optional `docker stats` CPU/memory snapshot when the docker CLI is available

The ingestion and reconciliation services must be running (docker compose up). Stop the three
simulators first if you want the measurement to include only load-test traffic.

Usage:
    python -m scripts.run_load_test --payments 20000 --rate 300 --burst-rate 1500 --burst-seconds 10
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from batch.daily_reconciliation import run_business_date
from common.config import (
    BusinessSettings,
    DatabaseSettings,
    KafkaSettings,
    ReconciliationSettings,
    SimulationSettings,
    TopicSettings,
    load,
)
from common.kafka_config import producer_config
from database.connection import connect
from generators.publisher import publish
from generators.simulation import build_all_events
from processing.case_manager import business_date
from processing.rules import load_rules
from scripts.report import kafka_lag

RESULTS_DIR = Path("load_test_results")


def docker_stats() -> list[dict[str, Any]] | str:
    if not shutil.which("docker"):
        return "docker CLI not available in this environment"
    try:
        out = subprocess.run(  # noqa: S603 - fixed argument list
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],  # noqa: S607
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout
        return [json.loads(line) for line in out.splitlines() if line.strip()]
    except (subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return f"docker stats failed: {exc}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline load test (synthetic data)")
    parser.add_argument("--payments", type=int, default=5000)
    parser.add_argument("--rate", type=float, default=200.0, help="steady events/second")
    parser.add_argument("--burst-rate", type=float, default=0.0, help="events/second during the initial burst")
    parser.add_argument("--burst-seconds", type=float, default=0.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--delayed-rate", type=float, default=0.05)
    parser.add_argument("--delay-max-s", type=float, default=30.0)
    parser.add_argument("--invalid-rate", type=float, default=0.003)
    parser.add_argument("--failure-rate", type=float, default=0.01, help="missing/mismatch injection per source")
    parser.add_argument("--wait-timeout", type=float, default=1800.0)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)

    run_id = args.run_id or f"LT{uuid.uuid4().hex[:8].upper()}"
    sim = load(
        SimulationSettings,
        run_id=run_id,
        payments_per_second=max(args.rate / 5.0, 1.0),
        duplicate_delivery_rate=args.duplicate_rate,
        delayed_event_rate=args.delayed_rate,
        delay_min_s=1.0,
        delay_max_s=args.delay_max_s,
        invalid_payload_rate=args.invalid_rate,
        missing_processor_rate=args.failure_rate / 2,
        missing_ledger_rate=args.failure_rate / 2,
        processor_amount_mismatch_rate=args.failure_rate / 2,
        ledger_amount_mismatch_rate=args.failure_rate / 2,
    )
    topics, kafka, db_settings = load(TopicSettings), load(KafkaSettings), load(DatabaseSettings)
    tz = load(BusinessSettings).tz
    rules = load_rules(load(ReconciliationSettings).rules_path)

    t_build = time.perf_counter()
    start_time = datetime.now(UTC)
    events = build_all_events(sim, topics, 0, args.payments, start_time)
    build_s = time.perf_counter() - t_build
    sources = {s: sum(1 for e in events if e.source_system == s) for s in ("mybell_app", "payment_processor", "billing_ledger")}
    print(f"[{run_id}] built {len(events)} events for {args.payments} payments in {build_s:.1f}s; publishing...")  # noqa: T201

    lag_samples: list[int] = []
    stop = threading.Event()

    def sample_lag() -> None:
        while not stop.is_set():
            try:
                lag_samples.append(sum(row[4] for row in kafka_lag()))
            except Exception:  # noqa: BLE001, S110 - sampling is best effort
                pass
            stop.wait(2.0)

    sampler = threading.Thread(target=sample_lag, daemon=True)
    sampler.start()

    producer = Producer(producer_config(kafka, f"load-test-{run_id}", linger_ms=10))
    test_started = datetime.now(UTC)
    burst = (args.burst_rate, args.burst_seconds) if args.burst_rate > 0 and args.burst_seconds > 0 else None
    stats = publish(producer, events, events_per_second=args.rate, burst=burst)
    print(f"[{run_id}] published {stats.delivered}/{stats.attempted} in {stats.duration_s:.1f}s ({stats.rate:.0f} ev/s)")  # noqa: T201

    pattern = f"PAY-{run_id}-%"
    deadline = time.monotonic() + args.wait_timeout
    # Bronze rows are attributed to the test by ingestion time (some malformed payloads carry no run id),
    # which is why the simulators must be stopped while the load test runs.
    with connect(db_settings, autocommit=True) as conn:
        while time.monotonic() < deadline:
            raw_row = conn.execute(
                "SELECT count(*) FROM raw_payment_events WHERE ingestion_time >= %s", (test_started,)
            ).fetchone()
            queue_row = conn.execute(
                "SELECT count(*) FROM reconciliation_work_queue WHERE payment_id LIKE %s", (pattern,)
            ).fetchone()
            assert raw_row is not None and queue_row is not None
            if raw_row[0] >= stats.delivered and queue_row[0] == 0:
                break
            time.sleep(2)
        drained_at = datetime.now(UTC)
        stop.set()
        stats_snapshot = docker_stats()

        ingest = conn.execute(
            """
            SELECT count(*), min(ingestion_time), max(ingestion_time),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM ingestion_time - kafka_timestamp) * 1000),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingestion_time - kafka_timestamp) * 1000),
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM ingestion_time - kafka_timestamp) * 1000),
                   count(*) FILTER (WHERE processing_status = 'QUARANTINED'),
                   count(*) FILTER (WHERE processing_status LIKE 'DUPLICATE%%')
            FROM raw_payment_events WHERE ingestion_time >= %s
            """,
            (test_started,),
        ).fetchone()
        recon = conn.execute(
            """
            WITH c AS (
                SELECT c.payment_id, c.first_seen_at, c.last_evaluated_at, c.reconciliation_status,
                       (SELECT max(r.ingestion_time) FROM raw_payment_events r
                         WHERE r.payment_id = c.payment_id AND r.processing_status = 'PROCESSED') AS last_ingested
                FROM reconciliation_cases c WHERE c.payment_id LIKE %s)
            SELECT count(*), min(first_seen_at), max(last_evaluated_at),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM last_evaluated_at - last_ingested) * 1000),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM last_evaluated_at - last_ingested) * 1000)
            FROM c
            """,
            (pattern,),
        ).fetchone()
        status_counts: dict[str, int] = dict(conn.execute(
            "SELECT reconciliation_status, count(*) FROM reconciliation_cases WHERE payment_id LIKE %s GROUP BY 1",
            (pattern,),
        ).fetchall())

    assert ingest is not None and recon is not None
    rows, first_ing, last_ing, k50, k95, k99, quarantined, duplicates = ingest
    ingest_s = (last_ing - first_ing).total_seconds() if first_ing and last_ing else 0.0
    n_cases, first_case, last_eval, r50, r95 = recon
    recon_s = (last_eval - first_case).total_seconds() if first_case and last_eval else 0.0

    batch_started = time.perf_counter()
    with connect(db_settings) as conn:
        summary = run_business_date(conn, rules, tz, business_date(start_time, tz), "DAILY", f"load-test:{run_id}")
    batch_s = time.perf_counter() - batch_started

    result = {
        "run_id": run_id,
        "executed_at": test_started.isoformat(),
        "host": {"platform": platform.platform(), "python": platform.python_version(), "machine": platform.machine()},
        "configuration": vars(args),
        "workload": {"payments": args.payments, "events_built": len(events), "events_by_source": sources},
        "producer": {"delivered": stats.delivered, "failed": stats.failed, "duration_s": round(stats.duration_s, 2),
                     "events_per_s": round(stats.rate, 1)},
        "ingestion": {"raw_rows": rows, "duration_s": round(ingest_s, 2),
                      "events_per_s": round(rows / ingest_s, 1) if ingest_s else None,
                      "kafka_to_bronze_ms": {"p50": k50, "p95": k95, "p99": k99},
                      "quarantined": quarantined, "duplicates_detected": duplicates},
        "reconciliation": {"cases": n_cases, "duration_s": round(recon_s, 2),
                           "cases_per_s": round(n_cases / recon_s, 1) if recon_s else None,
                           "last_event_to_case_ms": {"p50": r50, "p95": r95}, "status_counts": status_counts},
        "consumer_lag": {"max_messages": max(lag_samples) if lag_samples else None, "samples": len(lag_samples)},
        "drained_seconds_after_publish_start": round((drained_at - test_started).total_seconds(), 1),
        "daily_batch": {"duration_s": round(batch_s, 2), "payments_evaluated": summary.payments_evaluated,
                        "status": summary.status},
        "docker_stats": stats_snapshot,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{run_id}.json"
    path.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))  # noqa: T201
    print(f"results written to {path}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
