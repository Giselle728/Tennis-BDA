"""
Bloom Filter implementation for stream processing.

A Bloom Filter is a space-efficient probabilistic data structure
used to test whether an item has possibly been seen before.

Properties:
- False positives are possible.
- False negatives should not occur during normal operation.
- Does not store the original items.
"""

from __future__ import annotations

import hashlib
import math
from typing import Union


HashableItem = Union[str, int, float, bytes]


class BloomFilter:
    """Space-efficient probabilistic membership filter."""

    def __init__(
        self,
        capacity: int = 100_000,
        error_rate: float = 0.01,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be greater than zero")

        if not 0 < error_rate < 1:
            raise ValueError("error_rate must be between 0 and 1")

        self.capacity = capacity
        self.error_rate = error_rate

        self.size = max(
            1,
            int(
                -capacity * math.log(error_rate)
                / (math.log(2) ** 2)
            ),
        )

        self.num_hashes = max(
            1,
            int(
                (self.size / capacity) * math.log(2)
            ),
        )

        self.bit_array = bytearray(
            (self.size + 7) // 8
        )

        self.items_added = 0

    @staticmethod
    def _normalize_item(item: HashableItem) -> bytes:
        """Convert supported values into bytes."""
        if isinstance(item, bytes):
            return item

        return str(item).encode("utf-8")

    def _hash_indices(self, item: HashableItem):
        """
        Generate deterministic hash positions using double hashing.
        """
        data = self._normalize_item(item)

        digest_1 = hashlib.sha256(
            b"hash_1:" + data
        ).digest()

        digest_2 = hashlib.sha256(
            b"hash_2:" + data
        ).digest()

        hash_1 = int.from_bytes(
            digest_1[:8],
            byteorder="big",
        )

        hash_2 = int.from_bytes(
            digest_2[:8],
            byteorder="big",
        )

        for index in range(self.num_hashes):
            yield (
                hash_1 + index * hash_2
            ) % self.size

    def _get_bit(self, index: int) -> bool:
        """Read a bit from the bit array."""
        byte_index = index // 8
        bit_index = index % 8

        return bool(
            self.bit_array[byte_index]
            & (1 << bit_index)
        )

    def _set_bit(self, index: int) -> None:
        """Set a bit in the bit array."""
        byte_index = index // 8
        bit_index = index % 8

        self.bit_array[byte_index] |= (
            1 << bit_index
        )

    def add(self, item: HashableItem) -> None:
        """Add an item to the Bloom Filter."""
        for index in self._hash_indices(item):
            self._set_bit(index)

        self.items_added += 1

    def might_contain(self, item: HashableItem) -> bool:
        """
        Check whether an item may exist in the filter.

        Returns:
            False: Item is definitely not present.
            True: Item may be present.
        """
        return all(
            self._get_bit(index)
            for index in self._hash_indices(item)
        )

    def clear(self) -> None:
        """Clear all bits and reset the item counter."""
        self.bit_array = bytearray(
            (self.size + 7) // 8
        )

        self.items_added = 0

    def __contains__(self, item: HashableItem) -> bool:
        """Support: item in bloom_filter."""
        return self.might_contain(item)

    def __len__(self) -> int:
        """Return the number of add operations performed."""
        return self.items_added