"""
Flajolet-Martin approximate distinct counting.

The Flajolet-Martin algorithm estimates the number of distinct
elements in a stream using hashing and trailing zero counts.

This implementation uses multiple hash functions and the median
of group averages to improve stability over a single estimator.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from typing import Any


class FlajoletMartin:
    """Approximate distinct counter using Flajolet-Martin."""

    def __init__(
        self,
        num_hashes: int = 32,
    ) -> None:
        if num_hashes <= 0:
            raise ValueError(
                "num_hashes must be greater than zero"
            )

        self.num_hashes = num_hashes
        self.max_trailing_zeros = [0] * num_hashes

    @staticmethod
    def _normalize_item(item: Any) -> bytes:
        """Convert an item into deterministic bytes."""
        return str(item).encode("utf-8")

    @staticmethod
    def _trailing_zeros(value: int) -> int:
        """
        Count trailing zero bits in a positive integer.

        A zero hash is treated as having 64 trailing zeros.
        """
        if value == 0:
            return 64

        return (value & -value).bit_length() - 1

    def _hash(self, item: Any, seed: int) -> int:
        """Create a deterministic 64-bit hash."""
        data = (
            f"{seed}:".encode("utf-8")
            + self._normalize_item(item)
        )

        digest = hashlib.sha256(data).digest()

        return int.from_bytes(
            digest[:8],
            byteorder="big",
        )

    def add(self, item: Any) -> None:
        """Process one item from the stream."""
        for seed in range(self.num_hashes):
            hash_value = self._hash(item, seed)
            zeros = self._trailing_zeros(hash_value)

            if zeros > self.max_trailing_zeros[seed]:
                self.max_trailing_zeros[seed] = zeros

    def add_many(self, items) -> None:
        """Process multiple items."""
        for item in items:
            self.add(item)

    def estimate(self) -> int:
        """
        Estimate the number of distinct elements.

        Uses groups of hash functions and the median of their
        average estimates.
        """
        if not any(self.max_trailing_zeros):
            return 0

        estimates = []

        group_size = max(
            1,
            self.num_hashes // 4,
        )

        for start in range(
            0,
            self.num_hashes,
            group_size,
        ):
            group = self.max_trailing_zeros[
                start:start + group_size
            ]

            if not group:
                continue

            average_zeros = sum(group) / len(group)

            estimate = (
                (2 ** average_zeros)
                / 0.77351
            )

            estimates.append(estimate)

        if not estimates:
            return 0

        return max(
            0,
            int(round(statistics.median(estimates))),
        )

    def reset(self) -> None:
        """Reset the estimator."""
        self.max_trailing_zeros = [0] * self.num_hashes