# Standalone benchmark for the duplicate-column-name optimization.
#
# Run with:
#   python benchmarks/benchmark_dedupe_keys.py
#
# This file intentionally does not use the test_ prefix, so pytest does not
# collect it. The baseline is frozen here to make comparisons reproducible.
from __future__ import annotations

import statistics
import timeit
from collections.abc import Iterable

from sqlite_utils.utils import dedupe_keys


def baseline_dedupe_keys(keys: Iterable[str]) -> list[str]:
    """The implementation before the next-suffix cache optimization."""
    keys = list(keys)
    taken = set(keys)
    if len(taken) == len(keys):
        return keys
    seen: set[str] = set()
    result: list[str] = []
    for key in keys:
        if key in seen:
            new_key = key
            suffix = 2
            while new_key in seen or new_key in taken:
                new_key = f"{key}_{suffix}"
                suffix += 1
            key = new_key
        seen.add(key)
        result.append(key)
    return result


def duplicate_keys(size: int) -> list[str]:
    """Return ``size`` identical original keys."""
    return ["id"] * size


def reserved_suffix_keys(size: int) -> list[str]:
    """Return duplicate ``id`` keys plus genuinely reserved original suffixes."""
    # The first size entries are identical original keys. The remaining size
    # entries are distinct original suffixes id_2 through id_(size + 1), so
    # those suffixes are reserved before duplicate resolution begins.
    return ["id"] * size + [f"id_{suffix}" for suffix in range(2, size + 2)]


def measure(function, keys: list[str], repeats: int = 5) -> float:
    # Check output equality on every run, not only the timed invocation.
    expected = baseline_dedupe_keys(keys)
    timings = []
    for _ in range(repeats):
        actual = function(keys)
        if actual != expected:
            raise AssertionError(
                f"Output mismatch for {function.__name__}: {actual!r} != {expected!r}"
            )
        timings.append(timeit.timeit(lambda: function(keys), number=1))
    return statistics.median(timings)


def main() -> None:
    cases = [
        ("duplicates-500", duplicate_keys(500)),
        ("duplicates-2000", duplicate_keys(2000)),
        ("duplicates-5000", duplicate_keys(5000)),
        ("reserved-suffix-500", reserved_suffix_keys(500)),
        ("reserved-suffix-2000", reserved_suffix_keys(2000)),
    ]
    for name, keys in cases:
        baseline = measure(baseline_dedupe_keys, keys)
        current = measure(dedupe_keys, keys)
        print(f"{name}: baseline={baseline:.6f}s current={current:.6f}s")


if __name__ == "__main__":
    main()
