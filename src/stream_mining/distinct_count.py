"""
Exact distinct counting for streaming data.
"""

from __future__ import annotations

from typing import Any, Iterable


class DistinctCounter:
    """Maintain an exact count of unique items."""

    def __init__(
        self,
        items: Iterable[Any] | None = None,
    ) -> None:
        self._items: set[Any] = set()

        if items is not None:
            self.add_many(items)

    def add(self, item: Any) -> bool:
        """
        Add an item.

        Returns:
            True if the item was new.
            False if the item already existed.
        """
        before = len(self._items)
        self._items.add(item)

        return len(self._items) > before

    def add_many(self, items: Iterable[Any]) -> int:
        """
        Add multiple items.

        Returns:
            Number of newly added unique items.
        """
        new_items = 0

        for item in items:
            if self.add(item):
                new_items += 1

        return new_items

    def count(self) -> int:
        """Return the exact number of unique items."""
        return len(self._items)

    def contains(self, item: Any) -> bool:
        """Check whether an item has been observed."""
        return item in self._items

    def get_items(self) -> set[Any]:
        """Return a copy of all distinct items."""
        return set(self._items)

    def reset(self) -> None:
        """Remove all tracked items."""
        self._items.clear()

    def __len__(self) -> int:
        """Support len(counter)."""
        return self.count()