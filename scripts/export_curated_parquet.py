"""Export every curated JSONL ledger to compressed Parquet with row-count QC."""
from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def varchar_columns(path: Path) -> str:
    keys: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            keys.update(json.loads(line))
    return "{" + ",".join(f"{sql_string(key)}:'VARCHAR'" for key in sorted(keys)) + "}"


def main() -> None:
    curated = (ROOT / "data/curated").resolve(); target = curated / "parquet"; target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    outputs = []
    for source in sorted(curated.glob("*.jsonl")):
        destination = target / f"{source.stem}.parquet"
        temporary = destination.with_suffix(".parquet.tmp")
        native_parquet = curated / f"{source.stem}.parquet"
        if native_parquet.is_file():
            # New full-history collectors already write schema-stable Parquet
            # while streaming their JSONL. Preserve that exact artifact rather
            # than reparsing multi-million-row JSONL a second time.
            shutil.copyfile(native_parquet, temporary)
            schema_mode = "collector_native_parquet"
        else:
            all_varchar = "gamma" in source.stem
            columns = f", columns={varchar_columns(source)}" if all_varchar else ""
            query = (
                "SELECT * FROM read_json_auto("
                f"{sql_string(str(source))}, format='newline_delimited', union_by_name=true, "
                f"maximum_object_size=104857600, sample_size=-1{columns})"
            )
            connection.execute(
                f"COPY ({query}) TO {sql_string(str(temporary))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
            schema_mode = (
                "all_varchar_for_mutable_metadata" if all_varchar else "inferred"
            )
        temporary.replace(destination)
        rows = connection.execute(f"SELECT count(*) FROM read_parquet({sql_string(str(destination))})").fetchone()[0]
        outputs.append({"source": str(source), "parquet": str(destination), "rows": rows, "bytes": destination.stat().st_size, "schema_mode": schema_mode})
        print(f"Parquet {source.name}: {rows} rows", flush=True)
    common = target / "accountability_events.parquet"
    if common.is_file():
        rows = connection.execute(f"SELECT count(*) FROM read_parquet({sql_string(str(common))})").fetchone()[0]
        outputs.append({
            "source": "derived from QC-complete native Parquet ledgers",
            "parquet": str(common),
            "rows": rows,
            "bytes": common.stat().st_size,
            "schema_mode": "common_accountability_schema_v1",
        })
    manifest = {
        "dataset": "Curated Oracle accountability Parquet export",
        "generated_at_utc": datetime.now(UTC).isoformat(), "compression": "zstd",
        "files": outputs, "total_rows_across_tables": sum(row["rows"] for row in outputs),
        "note": "Counts are per table and must not be summed as unique accountability events.",
    }
    output = ROOT / "data/manifests/curated_parquet.json"
    output.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"); print(output)


if __name__ == "__main__":
    main()
