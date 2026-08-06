"""Fill a bounded, non-overlapping Tellor archive range ahead of the main scan.

This helper is intentionally collect-only. The ascending canonical collector
still performs whole-chain coverage QC and final output generation. Choose a
range far enough ahead that both processes cannot reach the same segment at
the same time.
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from scripts.ingest_tellor_reports_block_results import ROOT, collect_segment


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--start-height", type=int, required=True)
    parser.add_argument("--end-height", type=int, required=True)
    parser.add_argument("--cutoff-height", type=int, default=19_890_860)
    parser.add_argument("--segment-heights", type=int, default=2_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--source-address", action="append", default=[])
    args = parser.parse_args()
    if args.start_height < 1 or args.end_height > args.cutoff_height:
        raise SystemExit("bounded range is outside the fixed-cutoff chain")
    if args.start_height > args.end_height:
        raise SystemExit("--start-height must not exceed --end-height")
    if (args.start_height - 1) % args.segment_heights:
        raise SystemExit("--start-height must be a canonical segment boundary")
    if (
        args.end_height != args.cutoff_height
        and args.end_height % args.segment_heights
    ):
        raise SystemExit("--end-height must be a canonical segment end")

    raw_dir = (
        ROOT / "data/raw/tellor_layer/report_block_events_full"
    ).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    segments = [
        (start, min(start + args.segment_heights - 1, args.end_height))
        for start in range(
            args.start_height,
            args.end_height + 1,
            args.segment_heights,
        )
    ]
    # Descending collection maximizes the distance from the main ascending scan.
    segments.reverse()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {}
        for index, (start, end) in enumerate(segments):
            source_address = (
                args.source_address[index % len(args.source_address)]
                if args.source_address
                else None
            )
            future = executor.submit(
                collect_segment,
                args.rpc_url,
                source_address,
                args.timeout,
                start,
                end,
                args.cutoff_height,
                raw_dir,
            )
            futures[future] = (start, end)
        receipts = [future.result() for future in as_completed(futures)]
    scanned = sum(int(row["scanned_blocks"]) for row in receipts)
    expected = args.end_height - args.start_height + 1
    if scanned != expected:
        raise RuntimeError(f"bounded coverage mismatch: {scanned} != {expected}")
    print(
        json.dumps(
            {
                "start_height": args.start_height,
                "end_height": args.end_height,
                "segments": len(segments),
                "scanned_blocks": scanned,
                "reports": sum(int(row["reports"]) for row in receipts),
                "all_required_assertions_pass": True,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
