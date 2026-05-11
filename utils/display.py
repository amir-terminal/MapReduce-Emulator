"""
utils/display.py
================
Console output helpers — table rendering and section headers.

Keeping presentation logic here means emulator.py and the job modules
stay free of any print() calls, and the display format can be changed
in one place without touching business logic.
"""

from __future__ import annotations

from core.types import JobStats

_DIVIDER = "=" * 55


def section(title: str) -> None:
    """Print a labelled section header."""
    print(f"\n{_DIVIDER}\n  {title}\n{_DIVIDER}")


def table(
    headers:    list[str],
    rows:       list[tuple],
    col_widths: list[int],
) -> None:
    """
    Render a simple fixed-width table to stdout.

    Parameters
    ----------
    headers    : column header strings
    rows       : sequence of row tuples (one value per column)
    col_widths : character width for each column
    """
    indent = "  "
    header_row = indent + "  ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    sep_row    = indent + "  ".join("-" * w     for w in col_widths)
    print(header_row)
    print(sep_row)
    for row in rows:
        print(indent + "  ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))


def print_stats(history: list[JobStats]) -> None:
    """Print execution statistics for every job in *history*."""
    section("Execution Statistics")
    for stats in history:
        print(stats)
        print()
