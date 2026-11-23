#!/usr/bin/env python3
"""Extract timestamps from sealer/tapper logs and report latency statistics.

Each log line has the form:

    2026-11-23T08:01:37.782Z {"machineId": 1, ...}

The script reads the JSON part, takes ``payload.overloadTimestamp`` from the
sealer log and the event ``timestamp`` from the tapper log, and reports the
end-to-end latency for the matching scenario:

    tapper_timestamp - sealer_overloadTimestamp

The current tapper log has no ``scenarioId``, so records are paired in order
(the nth OVERLOAD with the nth tapper status event of the selected type). If
both logs contain ``scenarioId``, the script joins on that instead.

Example:
    python3 tools/analyze_latency.py --csv tools/run2_latency.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "src" / "logging" / "run2 - working"


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp such as 2026-11-23T08:01:37.782Z."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_line(line: str) -> tuple[str, dict[str, Any]] | None:
    """Split a log line into its written-at prefix and the JSON payload."""
    line = line.strip()
    if not line:
        return None

    prefix, _, payload = line.partition(" ")
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        print(f"warning: skipping malformed JSON: {line[:120]}", file=sys.stderr)
        return None

    if not isinstance(parsed, dict):
        print(f"warning: skipping non-object JSON: {line[:120]}", file=sys.stderr)
        return None

    return prefix, parsed


def read_events(
    path: Path,
    status: str | None,
    start_from_payload: bool,
) -> list[dict[str, Any]]:
    """Read relevant entries from one timestamped JSON log file."""
    events: list[dict[str, Any]] = []

    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            parsed = parse_line(line)
            if parsed is None:
                continue

            written_at, event = parsed
            payload = event.get("payload", {})

            if status is not None and payload.get("status") != status:
                continue

            event_timestamp = event.get("timestamp")
            if not event_timestamp:
                print(
                    f"warning: {path}:{line_number} has no event timestamp",
                    file=sys.stderr,
                )
                continue

            start_timestamp = payload.get("overloadTimestamp") if start_from_payload else None
            if start_from_payload and not start_timestamp:
                print(
                    f"warning: {path}:{line_number} has no overloadTimestamp",
                    file=sys.stderr,
                )
                continue

            events.append(
                {
                    "line": line_number,
                    "logged_at": written_at,
                    "event_timestamp": event_timestamp,
                    "start_timestamp": start_timestamp or event_timestamp,
                    "scenario_id": payload.get("scenarioId"),
                    "status": payload.get("status"),
                }
            )

    return events


def pair_events(
    sealer_events: list[dict[str, Any]],
    tapper_events: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Match sealer overloads with tapper events, by scenarioId when possible."""
    if len(sealer_events) != len(tapper_events):
        print(
            f"warning: {len(sealer_events)} sealer events but "
            f"{len(tapper_events)} tapper events; pairing the first "
            f"{min(len(sealer_events), len(tapper_events))}",
            file=sys.stderr,
        )

    if all(event["scenario_id"] for event in sealer_events) and all(
        event["scenario_id"] for event in tapper_events
    ):
        by_id = {event["scenario_id"]: event for event in tapper_events}
        pairs = [
            (event, by_id[event["scenario_id"]])
            for event in sealer_events
            if event["scenario_id"] in by_id
        ]
        if pairs:
            return pairs

    count = min(len(sealer_events), len(tapper_events))
    return list(zip(sealer_events[:count], tapper_events[:count]))


def percentile(sorted_values: list[float], percent: float) -> float:
    """Linear-interpolated percentile, e.g. percentile(values, 95)."""
    if not sorted_values:
        return float("nan")

    position = (len(sorted_values) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def print_statistics(latencies: list[float]) -> None:
    ordered = sorted(latencies)

    print(f"Scenarios: {len(ordered)}")
    print(f"Minimum:   {ordered[0]:.3f} ms")
    print(f"Maximum:   {ordered[-1]:.3f} ms")
    print(f"Mean:      {statistics.fmean(ordered):.3f} ms")
    print(f"Std dev:   {statistics.stdev(ordered):.3f} ms" if len(ordered) > 1 else "Std dev:   n/a")
    print(f"Median:    {statistics.median(ordered):.3f} ms")
    for percent in (50, 90, 95, 99):
        print(f"p{percent:<3}:       {percentile(ordered, percent):.3f} ms")

    negative = sum(value < 0 for value in ordered)
    if negative:
        print(f"warning: {negative} negative latencies", file=sys.stderr)


def write_csv(
    path: Path,
    pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    latencies: list[float],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "scenarioId",
                "sealerTimestamp",
                "tapperTimestamp",
                "latencyMs",
            ]
        )
        for (sealer, tapper), latency in zip(pairs, latencies):
            writer.writerow(
                [
                    sealer["scenario_id"] or "",
                    sealer["start_timestamp"],
                    tapper["event_timestamp"],
                    f"{latency:.3f}",
                ]
            )

    print(f"Wrote {len(pairs)} rows to {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sealer",
        type=Path,
        default=DEFAULT_DIR / "sealer.log",
        help="path to sealer.log",
    )
    parser.add_argument(
        "--tapper",
        type=Path,
        default=DEFAULT_DIR / "tapper1.log",
        help="path to a tapper log file",
    )
    parser.add_argument(
        "--tapper-status",
        default="IDLE",
        help="tapper status produced by STOP (default: IDLE)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="optional output path for per-scenario latencies",
    )
    args = parser.parse_args()

    for path in (args.sealer, args.tapper):
        if not path.exists():
            print(f"error: file not found: {path}", file=sys.stderr)
            return 1

    sealer_events = read_events(args.sealer, "OVERLOADED", start_from_payload=True)
    tapper_events = read_events(args.tapper, args.tapper_status, start_from_payload=False)

    if not sealer_events or not tapper_events:
        print("error: no matching events found in the logs", file=sys.stderr)
        return 1

    pairs = pair_events(sealer_events, tapper_events)
    latencies = [
        (parse_timestamp(tapper["event_timestamp"]) - parse_timestamp(sealer["start_timestamp"]))
        .total_seconds()
        * 1000
        for sealer, tapper in pairs
    ]

    print(f"Sealer events: {len(sealer_events)} (from {args.sealer})")
    print(f"Tapper events: {len(tapper_events)} (from {args.tapper})")
    print()
    print_statistics(latencies)

    if args.csv:
        write_csv(args.csv, pairs, latencies)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
