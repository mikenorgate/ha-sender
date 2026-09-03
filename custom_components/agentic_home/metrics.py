"""Runtime metrics tracking for the Agentic Home integration.

RuntimeMetrics objects are stored in hass.data[DOMAIN][entry_id]["metrics"]
for consumption by S06 sensor entities.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class RuntimeMetrics:
    """Thread-safe counters and status tracking for the HA integration runtime.

    Attributes
    ----------
    error_count : int
        Number of frame-build errors encountered.
    last_push_time : float
        Unix timestamp (from time.time()) of the last successful batch push.
    frames_pushed : int
        Total number of frames accepted by the ingress endpoint.
    frames_captured : int
        Total number of frames captured from the HA event bus.
    last_push_status : int | None
        HTTP status code of the last batch push (2xx = success, 4xx/5xx = failure).
    registry_push_count : int
        Total number of registry snapshot pushes (successful).
    registry_last_push_time : float
        Unix timestamp of the last registry push (from time.time()).
    registry_error_count : int
        Number of registry push errors encountered.
    last_error_msg : str
        Human-readable description of the most recent stream or registry error.
    last_error_time : float
        Unix timestamp of the most recent error (from time.time()).
    _frame_push_times : deque[float]
        Sliding window of push timestamps for computing push_rate.
        Bounded to maxlen=10000 to limit memory growth.
    _on_update : Callable | None
        Optional callback invoked after every state mutation, outside the lock.
    """

    error_count: int = 0
    last_push_time: float = 0.0
    frames_pushed: int = 0
    frames_captured: int = 0
    last_push_status: int | None = None
    registry_push_count: int = 0
    registry_last_push_time: float = 0.0
    registry_error_count: int = 0
    catalog_push_count: int = 0
    catalog_last_push_time: float = 0.0
    catalog_error_count: int = 0
    last_error_msg: str = ""
    last_error_time: float = 0.0
    _frame_push_times: deque[float] = field(
        default_factory=lambda: deque(maxlen=10000), repr=False
    )
    _on_update: Callable | None = field(default=None, repr=False)

    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    @property
    def push_rate(self) -> float:
        """Moving-average push rate over the last 60 seconds.

        Returns
        -------
        float
            Average pushes per second over the last 60 s, or 0.0 if fewer
            than two timestamps fall within the window.
        """
        now = time.time()
        cutoff = now - 60.0
        with self._lock:
            count = sum(1 for ts in self._frame_push_times if ts >= cutoff)
        if count < 2:
            return 0.0
        return count / 60.0

    def record_stream_error(self, msg: str) -> None:
        """Record a stream-related error.

        Sets last_error_msg to ``"stream: {msg}"``, records the current
        timestamp, increments error_count, and fires _on_update.

        Parameters
        ----------
        msg : str
            Human-readable error detail from the stream layer.
        """
        with self._lock:
            self.last_error_msg = f"stream: {msg}"
            self.last_error_time = time.time()
            self.error_count += 1
        self._on_update and self._on_update()

    def record_registry_error_msg(self, msg: str) -> None:
        """Record a registry-related error.

        Sets last_error_msg to ``"registry: {msg}"``, records the current
        timestamp, increments registry_error_count, and fires _on_update.

        Parameters
        ----------
        msg : str
            Human-readable error detail from the registry layer.
        """
        with self._lock:
            self.last_error_msg = f"registry: {msg}"
            self.last_error_time = time.time()
            self.registry_error_count += 1
        self._on_update and self._on_update()

    def increment_error(self) -> None:
        """Increment the frame-build error counter."""
        with self._lock:
            self.error_count += 1
        self._on_update and self._on_update()

    def record_push(self, count: int, status: int, timestamp: float) -> None:
        """Record a batch push result.

        Parameters
        ----------
        count : int
            Number of frames in the pushed batch.
        status : int
            HTTP status code from the ingress endpoint.
        timestamp : float
            Unix timestamp of the push (from time.time()).
        """
        with self._lock:
            self.frames_pushed += count
            self.last_push_time = timestamp
            self.last_push_status = status
            self._frame_push_times.append(timestamp)
        self._on_update and self._on_update()

    def increment_captured(self, n: int = 1) -> None:
        """Increment the captured frame counter.

        Parameters
        ----------
        n : int
            Number of frames to add (default 1).
        """
        with self._lock:
            self.frames_captured += n

    def snapshot(self) -> dict:
        """Return a plain-dict snapshot of current metrics for S06 sensor entities.

        Returns
        -------
        dict
            Snapshot with: error_count, last_push_time, frames_pushed,
            frames_captured, last_push_status, registry_push_count,
            registry_last_push_time, registry_error_count, last_error,
            last_error_time, push_rate. last_error is the canonical key used
            by SENSOR_KEYS; it holds the same value as last_error_msg.
        """
        with self._lock:
            return {
                "error_count": self.error_count,
                "last_push_time": self.last_push_time,
                "frames_pushed": self.frames_pushed,
                "frames_captured": self.frames_captured,
                "last_push_status": self.last_push_status,
                "registry_push_count": self.registry_push_count,
                "registry_last_push_time": self.registry_last_push_time,
                "registry_error_count": self.registry_error_count,
                "catalog_push_count": self.catalog_push_count,
                "catalog_last_push_time": self.catalog_last_push_time,
                "catalog_error_count": self.catalog_error_count,
                "last_error": self.last_error_msg,
                "last_error_msg": self.last_error_msg,
                "last_error_time": self.last_error_time,
                "push_rate": self.push_rate,
            }

    def record_registry_push(self, status: int, timestamp: float) -> None:
        """Record a registry snapshot push result.

        Parameters
        ----------
        status : int
            HTTP status code from the ingress registry endpoint.
        timestamp : float
            Unix timestamp of the push (from time.time()).
        """
        with self._lock:
            self.registry_push_count += 1
            self.registry_last_push_time = timestamp
            self.last_push_status = status
        self._on_update and self._on_update()

    def increment_registry_error(self) -> None:
        """Increment the registry push error counter."""
        with self._lock:
            self.registry_error_count += 1
        self._on_update and self._on_update()

    def record_catalog_push(self, status: int, timestamp: float) -> None:
        """Record a catalog push result.

        Parameters
        ----------
        status : int
            HTTP status code from the ingress stream endpoint.
        timestamp : float
            Unix timestamp of the push (from time.time()).
        """
        with self._lock:
            self.catalog_push_count += 1
            self.catalog_last_push_time = timestamp
            self.last_push_status = status
        self._on_update and self._on_update()

    def record_catalog_error_msg(self, msg: str) -> None:
        """Record a catalog-related error.

        Sets last_error_msg to ``"catalog: {msg}"``, records the current
        timestamp, increments catalog_error_count, and fires _on_update.

        Parameters
        ----------
        msg : str
            Human-readable error detail from the catalog layer.
        """
        with self._lock:
            self.last_error_msg = f"catalog: {msg}"
            self.last_error_time = time.time()
            self.catalog_error_count += 1
        self._on_update and self._on_update()

