"""Formatting of values for people: sizes, durations, tables."""

import re
import textwrap
import time
from typing import Any, List, Optional, Sequence

# Control characters (escape sequences, newlines, tabs), the invisible
# characters that reorder or hide text, and unpaired surrogates.
_UNSAFE = re.compile(
    r"[\x00-\x1f\x7f-\x9f\u200b-\u200f\u2028-\u202e\u2060-\u206f\ufeff\ud800-\udfff]"
)
_FORMULA_START = ("=", "+", "-", "@")
_NUMBER = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?%?$")


def _escaped(match: "re.Match[str]") -> str:
    code = ord(match.group())
    return "\\x%02x" % code if code < 256 else "\\u%04x" % code


def clean(text: Any) -> str:
    """``text`` with nothing in it that a terminal would act on rather than show.

    Job names, command lines and process names are written by whoever submits
    a job. Their control and invisible characters are replaced by the escape
    notation (``\\x1b``), so a name cannot colour or clear a terminal, break a
    table, or start a new line in something that will be copied and run.
    """
    text = str(text)
    if not _UNSAFE.search(text):
        return text
    return _UNSAFE.sub(_escaped, text)


def csv_text(text: str) -> str:
    """A text cell for a CSV file that a spreadsheet will not take for a formula.

    A cell that begins with ``=``, ``+``, ``-`` or ``@`` is evaluated by
    spreadsheet programs; a leading apostrophe makes it text. Plain numbers and
    a lone dash are left alone.
    """
    text = clean(text)
    if text.startswith(_FORMULA_START) and text != "-" and not _NUMBER.match(text):
        return "'" + text
    return text


def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


def fmt_date(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d", time.localtime(ts))


def fmt_age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 90:
        return "%d s" % seconds
    if seconds < 5400:
        return "%.0f min" % (seconds / 60.0)
    if seconds < 2 * 86400:
        return "%.1f h" % (seconds / 3600.0)
    return "%.1f d" % (seconds / 86400.0)


def fmt_int(value: Optional[float]) -> str:
    return "-" if value is None else "{:,}".format(int(value))


def fmt_bytes(value: Optional[float]) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return ("%.0f %s" if unit == "B" else "%.1f %s") % (size, unit)
        size /= 1024.0
    return "%d B" % value


def fmt_num(value: Optional[float], digits: int = 1) -> str:
    """A number with as many decimals as it needs to be told apart from zero."""
    if value is None:
        return "-"
    if value != 0 and abs(value) < 10 ** (-digits):
        return "%.2g" % value
    text = "%.*f" % (digits, value)
    return text.rstrip("0").rstrip(".") if "." in text else text


def fmt_gb(gb: Optional[float]) -> str:
    """Memory given in GB: ``12.4 GB``, ``850 MB``."""
    if gb is None:
        return "-"
    if gb < 1:
        return "%.0f MB" % max(gb * 1024, 1 if gb > 0 else 0)
    return "%s GB" % fmt_num(gb)


def fmt_mem_mb(mb: Optional[float]) -> str:
    """Memory given in MB, as a request is written: ``15 GB``, ``768 MB``, ``1.5 GB``."""
    if mb is None:
        return "-"
    if mb < 1024:
        return "%.0f MB" % mb
    gb = mb / 1024.0
    return ("%d GB" % gb) if abs(gb - round(gb)) < 1e-9 else ("%.1f GB" % gb)


def fmt_minutes(minutes: Optional[float]) -> str:
    """A duration given in minutes as ``D-HH:MM:SS`` (the form Slurm prints)."""
    if minutes is None:
        return "-"
    return fmt_seconds(minutes * 60.0)


def fmt_seconds(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"
    total = int(round(seconds))
    days, rest = divmod(total, 86400)
    hours, rest = divmod(rest, 3600)
    mins, secs = divmod(rest, 60)
    if days:
        return "%d-%02d:%02d:%02d" % (days, hours, mins, secs)
    return "%02d:%02d:%02d" % (hours, mins, secs)


def fmt_pct(fraction: Optional[float], signed: bool = False) -> str:
    if fraction is None:
        return "-"
    return ("%+.0f%%" if signed else "%.0f%%") % (100.0 * fraction)


def fmt_hours(hours: Optional[float]) -> str:
    """Resource-hours with thousands separators; one decimal below 100."""
    if hours is None:
        return "-"
    if abs(hours) >= 100:
        return "{:,.0f}".format(hours)
    return "{:,.1f}".format(hours)


def table(
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
    align: Optional[str] = None,
    indent: str = "",
) -> str:
    """Plain-text table. ``align`` has one ``l`` or ``r`` per column (default: left)."""
    cells: List[List[str]] = [[str(h) for h in headers]] + [
        ["" if c is None else clean(c) for c in row] for row in rows
    ]
    ncol = max(len(r) for r in cells)
    for row in cells:
        row.extend([""] * (ncol - len(row)))
    widths = [max(len(row[i]) for row in cells) for i in range(ncol)]
    align = (align or "") + "l" * ncol
    lines = []
    for row in cells:
        parts = [
            row[i].rjust(widths[i]) if align[i] == "r" else row[i].ljust(widths[i])
            for i in range(ncol)
        ]
        lines.append((indent + "  ".join(parts)).rstrip())
    return "\n".join(lines)


def wrap(text: str, width: int = 100, indent: str = "") -> str:
    """Wrap a paragraph for the terminal."""
    return textwrap.fill(text, width=width, initial_indent=indent, subsequent_indent=indent)


_MD_SPECIAL = re.compile(r"([|<\[`\\])")


def markdown_cell(value: Any) -> str:
    """A table cell that cannot end its cell or become a link, a tag or code when rendered."""
    return "" if value is None else _MD_SPECIAL.sub(r"\\\1", clean(value))


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[Any]], align: str = "") -> str:
    align = align + "l" * len(headers)
    out = ["| " + " | ".join(str(h) for h in headers) + " |"]
    out.append(
        "|" + "|".join("---:" if align[i] == "r" else "---" for i in range(len(headers))) + "|"
    )
    for row in rows:
        out.append("| " + " | ".join(markdown_cell(c) for c in row) + " |")
    return "\n".join(out)
