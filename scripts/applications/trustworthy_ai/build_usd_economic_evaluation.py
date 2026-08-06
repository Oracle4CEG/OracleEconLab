#!/usr/bin/env python3
"""Build timestamped USD action economics for the fixed UMA benchmark.

Historical Chainlink proxy state is queried at the exact Polygon dispute or
settlement block.  USDC token payoff and native MATIC Gas are converted
separately before they are combined in a preregistered private-verifier utility
scenario.  Investigation and capital opportunity costs remain explicit
scenario parameters rather than fabricated observations.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import pandas as pd
import requests


getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "data/applications/trustworthy_ai_challenge"
AUDIT = ROOT / "data/applications/trustworthy_ai_requirements_audit"
CURATED = ROOT / "data/curated/parquet"
OUT = ROOT / "data/applications/trustworthy_ai_usd_economics"
RAW = ROOT / "data/raw/polygon/trustworthy_ai_chainlink_prices_v1.jsonl.gz"
REPORT = ROOT / "reports/trustworthy_ai_usd_economics.md"
LATEST_ROUND_DATA = "0xfeaf968c"
FEEDS = {
    "MATIC_USD": {"proxy": "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0", "decimals": 8},
    "USDC_USD": {"proxy": "0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7", "decimals": 8},
}
FEED_DIRECTORY = "https://reference-data-directory.vercel.app/feeds-matic-mainnet.json"
INVESTIGATION_COSTS = [Decimal("0"), Decimal("5"), Decimal("25"), Decimal("100")]
CAPITAL_APRS = [Decimal("0"), Decimal("0.05"), Decimal("0.10"), Decimal("0.20")]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def env_url() -> str | None:
    path = ROOT / ".env"
    if not path.exists():
        return None
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:]
        if "=" in line and line.split("=", 1)[0].strip() == "NODE_URL2":
            return line.split("=", 1)[1].strip().strip("\"'")
    return None


def load_raw() -> dict[tuple[str, int], dict[str, Any]]:
    if not RAW.exists():
        return {}
    rows = {}
    with gzip.open(RAW, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            rows[(row["feed"], int(row["block_number"]))] = row
    return rows


def rpc_call(url: str, feed: str, block: int, attempts: int = 5) -> dict[str, Any]:
    proxy = FEEDS[feed]["proxy"]
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.post(url, json={
                "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                "params": [{"to": proxy, "data": LATEST_ROUND_DATA}, hex(block)],
            }, timeout=45)
            response.raise_for_status()
            body = response.json()
            if body.get("error") or not body.get("result"):
                raise RuntimeError(str(body.get("error") or "empty result"))
            return {
                "feed": feed, "proxy": proxy, "decimals": FEEDS[feed]["decimals"],
                "block_number": block, "eth_call_result": body["result"],
                "retrieved_at_utc": datetime.now(UTC).isoformat(),
            }
        except Exception as exc:
            error = exc
            time.sleep(min(5.0, 0.25 * 2**attempt))
    raise RuntimeError(f"Chainlink call failed for {feed}@{block}: {error}")


def decode(row: dict[str, Any]) -> dict[str, Any]:
    value = row["eth_call_result"].removeprefix("0x")
    if len(value) < 64 * 5:
        raise RuntimeError("latestRoundData response too short")
    words = [value[index:index + 64] for index in range(0, 64 * 5, 64)]
    answer = int(words[1], 16)
    if answer >= 2**255:
        answer -= 2**256
    return {
        **row, "round_id": str(int(words[0], 16)), "answer_raw": str(answer),
        "started_at_unix": int(words[2], 16), "updated_at_unix": int(words[3], 16),
        "answered_in_round": str(int(words[4], 16)),
    }


def load_targets() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    consequences = pd.read_parquet(AUDIT / "observed_economic_consequences.parquet")
    predictions = pd.read_parquet(BASE / "predictions.parquet")
    rounds = pd.read_parquet(CURATED / "polygon_uma_request_rounds.parquet")
    rounds = rounds[rounds.oo_request_id.isin(consequences.sample_id)][[
        "oo_request_id", "dispute_tx", "settlement_block"
    ]]
    events = pd.read_parquet(CURATED / "polygon_oov2_events.parquet")
    times = events[events.oo_request_id.isin(consequences.sample_id)].pivot_table(
        index="oo_request_id", columns="event", values="block_time", aggfunc="max"
    ).reset_index()
    frame = consequences.merge(rounds, left_on="sample_id", right_on="oo_request_id", validate="one_to_one").merge(
        times[["oo_request_id", "DisputePrice", "Settle"]], on="oo_request_id", validate="one_to_one"
    )
    return frame, predictions, pd.read_parquet(BASE / "splits.parquet")


def freeze_prices(frame: pd.DataFrame, offline: bool, workers: int) -> dict[tuple[str, int], dict[str, Any]]:
    targets: set[tuple[str, int]] = set()
    for row in frame.itertuples(index=False):
        targets.add(("MATIC_USD", int(row.receipt_block_number)))
        targets.add(("USDC_USD", int(row.receipt_block_number)))
        targets.add(("USDC_USD", int(row.settlement_block)))
    cached = load_raw()
    missing = sorted(targets - set(cached))
    if missing and offline:
        raise RuntimeError(f"offline Chainlink cache incomplete: {len(missing)} calls missing")
    if missing:
        url = env_url()
        if not url:
            raise RuntimeError("NODE_URL2 required for historical Polygon Chainlink calls")
        chain = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, timeout=20).json().get("result")
        if chain != "0x89":
            raise RuntimeError("NODE_URL2 must be Polygon chain 137")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(rpc_call, url, feed, block): (feed, block) for feed, block in missing}
            for index, future in enumerate(as_completed(futures), 1):
                cached[futures[future]] = future.result()
                if index % 250 == 0:
                    print(f"price_calls={index}/{len(missing)}", flush=True)
    selected = [cached[key] for key in sorted(targets)]
    RAW.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(RAW, "wt", encoding="utf-8", compresslevel=9) as handle:
        for row in selected:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    return {key: decode(cached[key]) for key in targets}


def d(value: Any) -> Decimal:
    return Decimal(str(value))


def money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.00000001")), "f")


def build_price_evidence(frame: pd.DataFrame, prices: dict[tuple[str, int], dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for row in frame.itertuples(index=False):
        for purpose, feed, block, event_time in [
            ("dispute_gas", "MATIC_USD", int(row.receipt_block_number), int(row.DisputePrice)),
            ("dispute_capital", "USDC_USD", int(row.receipt_block_number), int(row.DisputePrice)),
            ("settlement_payoff", "USDC_USD", int(row.settlement_block), int(row.Settle)),
        ]:
            price = prices[(feed, block)]
            rows.append({
                "sample_id": row.sample_id, "purpose": purpose, "feed": feed,
                "proxy": price["proxy"].lower(), "block_number": block,
                "event_time_unix": event_time, "round_id": price["round_id"],
                "answer_raw": price["answer_raw"], "decimals": price["decimals"],
                "price_usd": money(d(price["answer_raw"]) / d(10 ** price["decimals"])),
                "updated_at_unix": price["updated_at_unix"],
                "price_age_seconds": event_time - price["updated_at_unix"],
                "available_at_event": price["updated_at_unix"] <= event_time,
                "source": "Chainlink historical proxy state via Polygon archive eth_call",
            })
    return pd.DataFrame(rows)


def base_usd(frame: pd.DataFrame, evidence: pd.DataFrame) -> pd.DataFrame:
    price = evidence.pivot(index="sample_id", columns="purpose", values="price_usd")
    rows = []
    for row in frame.itertuples(index=False):
        p = price.loc[row.sample_id]
        token_payoff_usd = d(row.observed_challenge_token_payoff_raw) / d(10**row.currency_decimals) * d(p.settlement_payoff)
        gas_usd = d(row.observed_challenge_gas_cost_native_raw) / d(10**row.gas_native_decimals) * d(p.dispute_gas)
        capital_usd_days = d(row.challenge_capital_days_locked_raw) / d(10**row.currency_decimals) * d(p.dispute_capital)
        rows.append({
            "sample_id": row.sample_id, "challenge_token_payoff_usd": money(token_payoff_usd),
            "challenge_gas_cost_usd": money(gas_usd),
            "challenge_capital_usd_days": money(capital_usd_days),
            "usdc_settlement_price_usd": p.settlement_payoff,
            "usdc_dispute_price_usd": p.dispute_capital, "matic_dispute_price_usd": p.dispute_gas,
            "payoff_price_time": "settlement", "gas_and_capital_price_time": "dispute",
        })
    return pd.DataFrame(rows)


def scenario_evaluation(predictions: pd.DataFrame, usd: pd.DataFrame) -> pd.DataFrame:
    frame = predictions.merge(usd, on="sample_id", validate="many_to_one")
    rows = []
    for row in frame.itertuples(index=False):
        payoff = d(row.challenge_token_payoff_usd)
        gas = d(row.challenge_gas_cost_usd)
        capital_days = d(row.challenge_capital_usd_days)
        for investigation in INVESTIGATION_COSTS:
            for apr in CAPITAL_APRS:
                capital_cost = capital_days * apr / d(365)
                challenge_utility = payoff - gas - capital_cost - investigation
                accept_utility = Decimal(0)
                abstain_utility = Decimal(0)
                # Perfect-information upper bound after paying the investigation cost.
                investigate_utility = max(Decimal(0), payoff - gas - capital_cost) - investigation
                utilities = {
                    "Accept": accept_utility, "Abstain": abstain_utility,
                    "Challenge": challenge_utility, "Investigate": investigate_utility,
                }
                chosen = utilities[row.action]
                best_action, best = max(utilities.items(), key=lambda item: item[1])
                rows.append({
                    "sample_id": row.sample_id, "model": row.model, "action": row.action,
                    "investigation_cost_usd_scenario": money(investigation),
                    "capital_apr_scenario": str(apr), "capital_cost_usd": money(capital_cost),
                    "challenge_utility_usd": money(challenge_utility),
                    "investigate_utility_usd": money(investigate_utility),
                    "chosen_action_utility_usd": money(chosen), "best_action": best_action,
                    "best_action_utility_usd": money(best), "economic_regret_usd": money(best - chosen),
                    "utility_scope": "private verifier; historical Chainlink FX; investigation/APR scenario; no external social harm",
                })
    return pd.DataFrame(rows)


def summarize(scenarios: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, part in scenarios.groupby(["model", "investigation_cost_usd_scenario", "capital_apr_scenario"]):
        regret = pd.to_numeric(part.economic_regret_usd)
        utility = pd.to_numeric(part.chosen_action_utility_usd)
        rows.append({
            "model": keys[0], "investigation_cost_usd_scenario": keys[1], "capital_apr_scenario": keys[2],
            "observations": len(part), "mean_regret_usd": float(regret.mean()),
            "median_regret_usd": float(regret.median()), "mean_net_utility_usd": float(utility.mean()),
            "optimal_action_rate": float((regret == 0).mean()),
            "human_review_burden": float(part.action.isin(["Investigate", "Abstain"]).mean()),
        })
    return pd.DataFrame(rows)


def manual_review_scenarios(predictions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for model, part in predictions.groupby("model"):
        reviews = int(part.action.isin(["Investigate", "Abstain"]).sum())
        for minutes in [5, 15, 60]:
            for wage in [25, 50, 100]:
                rows.append({
                    "model": model, "test_observations": len(part), "review_observations": reviews,
                    "review_rate": reviews / len(part), "minutes_per_review_scenario": minutes,
                    "reviewer_hourly_cost_usd_scenario": wage,
                    "total_manual_review_cost_usd": reviews * minutes / 60 * wage,
                })
    return pd.DataFrame(rows)


def validate(evidence: pd.DataFrame, usd: pd.DataFrame, scenarios: pd.DataFrame) -> dict[str, bool]:
    checks = {
        "all_2430_prices_present": len(evidence) == 2430,
        "all_prices_pre_or_at_event": evidence.available_at_event.all(),
        "all_prices_positive": pd.to_numeric(evidence.price_usd).gt(0).all(),
        "all_price_ages_nonnegative": evidence.price_age_seconds.ge(0).all(),
        "all_810_usd_rows": len(usd) == 810,
        "all_17920_scenario_rows": len(scenarios) == 17920,
        "all_regret_nonnegative": pd.to_numeric(scenarios.economic_regret_usd).ge(0).all(),
        "utility_scope_declared": scenarios.utility_scope.str.contains("private verifier").all(),
    }
    if not all(checks.values()):
        raise RuntimeError(f"USD economic QC failed: {checks}")
    return {key: bool(value) for key, value in checks.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    frame, predictions, _ = load_targets()
    prices = freeze_prices(frame, args.offline, args.workers)
    evidence = build_price_evidence(frame, prices)
    usd = base_usd(frame, evidence)
    scenarios = scenario_evaluation(predictions, usd)
    summary = summarize(scenarios)
    review = manual_review_scenarios(predictions)
    checks = validate(evidence, usd, scenarios)
    objects = {
        "chainlink_price_evidence": evidence, "observed_usd_economics": usd,
        "economic_regret_scenarios": scenarios, "economic_regret_summary": summary,
        "manual_review_cost_scenarios": review,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    for name, value in objects.items():
        value.to_parquet(OUT / f"{name}.parquet", index=False)
        value.to_csv(OUT / f"{name}.csv", index=False)
    base = summary[(summary.investigation_cost_usd_scenario == "25.00000000") & (summary.capital_apr_scenario == "0.10")]
    REPORT.write_text(
        "# Timestamped USD economic evaluation\n\n"
        f"- Historical Chainlink price observations: **{len(evidence):,}**; all are from proxy state at the exact event block and have `updatedAt <= event_time`.\n"
        f"- Exact observed USD component rows: **{len(usd):,}/810**.\n"
        f"- Regret scenario rows: **{len(scenarios):,}** across 4 investigation-cost and 4 capital-APR assumptions.\n"
        "- USDC payoff, MATIC Gas and capital opportunity cost are converted independently before combination.\n"
        "- Investigation cost and capital APR are scenarios, not observed facts; utility is private-verifier utility, not social welfare.\n\n"
        "## Base scenario ($25 investigation; 10% APR)\n\n" +
        base[["model", "mean_regret_usd", "mean_net_utility_usd", "optimal_action_rate", "human_review_burden"]].to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    files = []
    for path in sorted(OUT.glob("*")):
        if path.is_file() and path.name != "manifest.json":
            files.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)})
    files.extend([
        {"path": str(RAW), "bytes": RAW.stat().st_size, "sha256": sha256(RAW)},
        {"path": str(REPORT), "bytes": REPORT.stat().st_size, "sha256": sha256(REPORT)},
    ])
    (OUT / "manifest.json").write_text(json.dumps({
        "dataset": "Timestamped USD Trustworthy AI economics", "version": "1.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(), "feed_directory": FEED_DIRECTORY,
        "feed_proxies": FEEDS, "all_required_assertions_pass": True,
        "checks": checks, "rows": {k: len(v) for k, v in objects.items()}, "files": files,
    }, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"checks": checks, "rows": {k: len(v) for k, v in objects.items()}}, indent=2))


if __name__ == "__main__":
    main()
