"""
Sliding window implementation for streaming events.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Iterable


class SlidingWindow:
    """
    Maintain the latest N events in a fixed-size window.

    When the window reaches capacity, adding a new event removes
    the oldest event automatically.
    """

    def __init__(
        self,
        max_size: int = 100,
        items: Iterable[Any] | None = None,
    ) -> None:
        if max_size <= 0:
            raise ValueError(
                "max_size must be greater than zero"
            )

        self.max_size = max_size

        self._window: deque[Any] = deque(
            maxlen=max_size
        )

        if items is not None:
            self.add_many(items)

    def add(self, event: Any) -> None:
        """Add one event to the window."""
        self._window.append(event)

    def add_many(self, events: Iterable[Any]) -> None:
        """Add multiple events sequentially."""
        for event in events:
            self.add(event)

    def get_events(self) -> list[Any]:
        """Return events from oldest to newest."""
        return list(self._window)

    def latest(self) -> Any | None:
        """Return the newest event, if available."""
        if not self._window:
            return None

        return self._window[-1]

    def oldest(self) -> Any | None:
        """Return the oldest event, if available."""
        if not self._window:
            return None

        return self._window[0]

    def size(self) -> int:
        """Return the current number of events."""
        return len(self._window)

    def is_full(self) -> bool:
        """Check whether the window has reached capacity."""
        return self.size() == self.max_size

    def clear(self) -> None:
        """Remove all events."""
        self._window.clear()

    def __len__(self) -> int:
        """Support len(window)."""
        return self.size()

    def __iter__(self):
        """Iterate over events from oldest to newest."""
        return iter(self._window)