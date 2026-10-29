"""Graceful shutdown signal handling."""

from __future__ import annotations

import logging
import signal
import threading
from typing import Any

log = logging.getLogger(__name__)


def install_signal_handlers(stop_event: threading.Event) -> None:
    def handler(signum: int, _frame: Any) -> None:
        log.info("shutdown signal received", extra={"signal": signum})
        stop_event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
