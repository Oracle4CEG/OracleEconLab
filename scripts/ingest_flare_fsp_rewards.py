"""Collect Flare FSP/FTSOv2 reward and minimum-condition evidence.

The official ``flare-foundation/fsp-rewards`` repository is the canonical
calculation output.  Every published Merkle root is reconciled to
FlareSystemsManager state at the fixed cutoff block.  Reward claims are kept as
aggregate FSP entitlements because the Merkle tree combines several protocols;
they are not relabeled as pure FTSO accuracy rewards.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests

from oracle_ledger.flare_fsp import (
    CLAIM_TYPES,
    FlareRpc,
    bytes20_call_data,
    iso_timestamp,
    reward_epoch_bounds,
    uint256_call_data,
)


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
FIRST_EPOCH = 228
LAST_EPOCH = 410
CUTOFF_ACTIVE_EPOCH = 411
FIRST_MINIMUM_CONDITIONS_EPOCH = 251
REPOSITORY = "flare-foundation/fsp-rewards"
DEFAULT_RPC = "https://flare-api.flare.network/ext/C/rpc"
FLARE_SYSTEMS_MANAGER = "0x89e50DC0380e597ecE79c8494bAAFD84537AD0D4"
VOTER_REGISTRY = "0x2580101692366e2f331e891180d9ffdF861Fce83"
REWARDS_HASH_SELECTOR = "0x647006e2"
CHILLED_UNTIL_SELECTOR = "0x3c5cb76f"
REQUIRED_BASE_FILES = ("reward-distribution-data.json", "reward-epoch-info.json")
REQUIRED_CONDITION_FILES = ("minimal-conditions.json", "passes.json")


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    temporary.replace(path)
    return count


def download_json(url: str, path: Path, expected_size: int) -> dict[str, Any]:
    if path.is_file() and path.stat().st_size == expected_size:
        return {"path": str(path), "bytes": expected_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "cache": True}
    last_error: Exception | None = None
    for attempt in range(6):
        try:
            response = requests.get(url, timeout=120, headers={"User-Agent": "oracle-accountability-atlas/0.1"})
            response.raise_for_status()
            content = response.content
            if len(content) != expected_size:
                raise RuntimeError(f"size mismatch for {url}: {len(content)} != {expected_size}")
            json.loads(content)
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(content)
            temporary.replace(path)
            return {"path": str(path), "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(), "cache": False}
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < 5:
                time.sleep(min(2**attempt, 20))
    raise RuntimeError(f"failed to download {url}") from last_error


def batches(values: list[Any], size: int = 50) -> Iterable[list[Any]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def safe_ratio_ppm(numerator: int, denominator: int) -> int | None:
    return numerator * 1_000_000 // denominator if denominator else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect strict-cutoff Flare FSP/FTSOv2 reward evidence")
    parser.add_argument("--rpc-url", default=os.getenv("FLARE_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    raw_dir = (ROOT / "data/raw/flare_fsp").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)

    commit_response = requests.get(
        f"https://api.github.com/repos/{REPOSITORY}/commits/main",
        timeout=60,
        headers={"User-Agent": "oracle-accountability-atlas/0.1"},
    )
    commit_response.raise_for_status()
    source_commit = commit_response.json()["sha"]
    tree_response = requests.get(
        f"https://api.github.com/repos/{REPOSITORY}/git/trees/{source_commit}?recursive=1",
        timeout=60,
        headers={"User-Agent": "oracle-accountability-atlas/0.1"},
    )
    tree_response.raise_for_status()
    tree = tree_response.json()
    if tree.get("truncated"):
        raise RuntimeError("official reward repository tree was truncated")
    blobs = {row["path"]: row for row in tree["tree"] if row.get("type") == "blob"}

    needed: list[tuple[int, str, int]] = []
    for epoch in range(FIRST_EPOCH, LAST_EPOCH + 1):
        names = list(REQUIRED_BASE_FILES)
        if epoch >= FIRST_MINIMUM_CONDITIONS_EPOCH:
            names.extend(REQUIRED_CONDITION_FILES)
        for name in names:
            relative = f"flare/{epoch}/{name}"
            if relative not in blobs:
                raise RuntimeError(f"official reward file missing: {relative}")
            needed.append((epoch, name, int(blobs[relative]["size"])))

    downloads: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {}
        for epoch, name, size in needed:
            relative = f"flare/{epoch}/{name}"
            url = f"https://raw.githubusercontent.com/{REPOSITORY}/{source_commit}/{relative}"
            future = executor.submit(download_json, url, raw_dir / str(epoch) / name, size)
            futures[future] = relative
        for completed, future in enumerate(as_completed(futures), 1):
            relative = futures[future]
            downloads[relative] = future.result()
            if completed % 100 == 0 or completed == len(futures):
                print(f"downloaded/validated {completed}/{len(futures)} official files", flush=True)

    rpc = FlareRpc(args.rpc_url)
    chain_id = int(rpc.call("eth_chainId", []), 16)
    if chain_id != 14:
        raise RuntimeError(f"expected Flare chain id 14, got {chain_id}")
    cutoff_block, cutoff_header = rpc.block_at_or_before(int(CUTOFF.timestamp()))
    cutoff_block_hex = hex(cutoff_block)
    if int(cutoff_header["timestamp"], 16) > int(CUTOFF.timestamp()):
        raise RuntimeError("cutoff block is after fixed cutoff")
    for address in (FLARE_SYSTEMS_MANAGER, VOTER_REGISTRY):
        if rpc.call("eth_getCode", [address, cutoff_block_hex]) == "0x":
            raise RuntimeError(f"expected contract code at cutoff: {address}")

    root_calls = [
        [{"to": FLARE_SYSTEMS_MANAGER, "data": uint256_call_data(REWARDS_HASH_SELECTOR, epoch)}, cutoff_block_hex]
        for epoch in range(FIRST_EPOCH, LAST_EPOCH + 1)
    ]
    onchain_roots: list[str] = []
    for group in batches(root_calls):
        onchain_roots.extend(rpc.batch("eth_call", group))
    root_by_epoch = dict(zip(range(FIRST_EPOCH, LAST_EPOCH + 1), onchain_roots, strict=True))

    epochs: list[dict[str, Any]] = []
    claims: list[dict[str, Any]] = []
    registrations: list[dict[str, Any]] = []
    conditions: list[dict[str, Any]] = []
    feed_performance: list[dict[str, Any]] = []
    pass_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    all_beneficiary_roles: dict[str, set[str]] = defaultdict(set)
    claim_types: Counter[str] = Counter()
    amount_by_type: Counter[str] = Counter()
    condition_mismatches = 0
    feed_total_mismatches = 0
    registration_link_misses = 0

    for epoch in range(FIRST_EPOCH, LAST_EPOCH + 1):
        distribution = json.loads((raw_dir / str(epoch) / "reward-distribution-data.json").read_text(encoding="utf-8"))
        info = json.loads((raw_dir / str(epoch) / "reward-epoch-info.json").read_text(encoding="utf-8"))
        if int(distribution["rewardEpochId"]) != epoch or int(info["rewardEpochId"]) != epoch:
            raise RuntimeError(f"reward epoch id mismatch in epoch {epoch}")
        if distribution["network"] != "flare":
            raise RuntimeError(f"unexpected network in epoch {epoch}: {distribution['network']}")
        start_ts, end_ts = reward_epoch_bounds(epoch)
        expected_start_round = epoch * 3_360
        expected_end_round = expected_start_round + 3_359
        round_qc = int(info["expectedStartVotingRoundId"]) == expected_start_round and int(info["endVotingRoundId"]) == expected_end_round
        published_root = distribution["merkleRoot"].lower()
        root_matches = root_by_epoch[epoch].lower() == published_root
        registrations_in_epoch = info.get("voterRegistrationInfo") or []
        registered_voters: set[str] = set()
        for index, wrapper in enumerate(registrations_in_epoch):
            registered = wrapper.get("voterRegistered") or {}
            detail = wrapper.get("voterRegistrationInfo") or {}
            voter = str(registered.get("voter") or detail.get("voter")).lower()
            registered_voters.add(voter)
            node_ids = [str(value).lower() for value in detail.get("nodeIds") or []]
            registrations.append({
                "reward_epoch_id": epoch,
                "registration_index": index,
                "voter_address": voter,
                "signing_policy_address": registered.get("signingPolicyAddress"),
                "submit_address": registered.get("submitAddress"),
                "submit_signatures_address": registered.get("submitSignaturesAddress"),
                "delegation_address": detail.get("delegationAddress"),
                "delegation_fee_bips": detail.get("delegationFeeBIPS"),
                "registration_weight_raw": registered.get("registrationWeight"),
                "wnat_weight_raw": detail.get("wNatWeight"),
                "wnat_capped_weight_raw": detail.get("wNatCappedWeight"),
                "node_ids": node_ids,
                "node_weights_raw": detail.get("nodeWeights") or [],
                "source_file": f"flare/{epoch}/reward-epoch-info.json",
            })
            for value, role in [(voter, "voter"), (detail.get("delegationAddress"), "delegation")]:
                if value:
                    all_beneficiary_roles[str(value).lower()].add(role)
            for node_id in node_ids:
                all_beneficiary_roles[node_id].add("node_id")

        epoch_claims = distribution.get("rewardClaims") or []
        weight_claim_count = 0
        for index, claim in enumerate(epoch_claims):
            body = claim["body"]
            claim_type_id = int(body["claimType"])
            claim_type = CLAIM_TYPES.get(claim_type_id, f"UNKNOWN_{claim_type_id}")
            amount = str(body["amount"])
            if not amount.isdigit():
                raise RuntimeError(f"non-integer reward amount in epoch {epoch}")
            if claim_type in {"WNAT", "MIRROR", "CCHAIN"}:
                weight_claim_count += 1
            claim_types[claim_type] += 1
            amount_by_type[claim_type] += int(amount)
            claims.append({
                "reward_epoch_id": epoch,
                "claim_index": index,
                "beneficiary": str(body["beneficiary"]).lower(),
                "claim_type_id": claim_type_id,
                "claim_type": claim_type,
                "amount_raw": amount,
                "asset": "FLR",
                "asset_decimals": 18,
                "is_weight_based": claim_type in {"WNAT", "MIRROR", "CCHAIN"},
                "merkle_proof_length": len(claim.get("merkleProof") or []),
                "merkle_root": published_root,
                "epoch_end_time_unix": end_ts,
                "attribution_scope": "aggregate_fsp_entitlement_not_ftso_component",
                "source_file": f"flare/{epoch}/reward-distribution-data.json",
            })
        weight_qc = weight_claim_count == int(distribution["noOfWeightBasedClaims"])

        condition_count = 0
        failure_count = 0
        if epoch >= FIRST_MINIMUM_CONDITIONS_EPOCH:
            minimum = json.loads((raw_dir / str(epoch) / "minimal-conditions.json").read_text(encoding="utf-8"))
            pass_data = json.loads((raw_dir / str(epoch) / "passes.json").read_text(encoding="utf-8"))
            pass_by_voter = {str(row["voterAddress"]).lower(): row for row in pass_data}
            condition_count = len(minimum)
            if len(pass_by_voter) != len(pass_data) or len(minimum) != len(pass_data):
                condition_mismatches += 1
            for row in minimum:
                voter = str(row["voterAddress"]).lower()
                if voter not in registered_voters:
                    registration_link_misses += 1
                pass_row = pass_by_voter.get(voter)
                if not pass_row or bool(pass_row["eligibleForReward"]) != bool(row["eligibleForReward"]):
                    condition_mismatches += 1
                ftso = row.get("ftsoScaling") or {}
                fast_updates = row.get("fastUpdates") or {}
                staking = row.get("staking") or {}
                fdc = row.get("fdc") or {}
                feed_rows = ftso.get("feedHits") or []
                if feed_rows and (
                    sum(int(item["feedHits"]) for item in feed_rows) != int(ftso["totalHits"])
                    or sum(int(item["totalHits"]) for item in feed_rows) != int(ftso["allPossibleHits"])
                ):
                    feed_total_mismatches += 1
                conditions.append({
                    "reward_epoch_id": epoch,
                    "data_provider_name": row.get("dataProviderName"),
                    "voter_address": voter,
                    "delegation_address": row.get("delegationAddress"),
                    "node_ids": row.get("nodeIds") or [],
                    "voter_index": row.get("voterIndex"),
                    "passes_held": row.get("passesHeld"),
                    "pass_earned": row.get("passEarned"),
                    "strikes": row.get("strikes"),
                    "eligible_for_reward": row.get("eligibleForReward"),
                    "new_number_of_passes": row.get("newNumberOfPasses"),
                    "ftso_scaling_condition_met": ftso.get("conditionMet"),
                    "ftso_total_hits": ftso.get("totalHits"),
                    "ftso_all_possible_hits": ftso.get("allPossibleHits"),
                    "ftso_hit_rate_ppm": safe_ratio_ppm(int(ftso.get("totalHits") or 0), int(ftso.get("allPossibleHits") or 0)),
                    "fast_updates_condition_met": fast_updates.get("conditionMet"),
                    "fast_updates_count": fast_updates.get("updates"),
                    "fast_updates_expected": fast_updates.get("expectedUpdates"),
                    "staking_condition_met": staking.get("conditionMet"),
                    "staking_obstructs_pass": staking.get("obstructsPass"),
                    "fdc_condition_met": fdc.get("conditionMet"),
                    "fdc_rewarded_voting_rounds": fdc.get("rewardedVotingRounds"),
                    "fdc_total_rewarded_voting_rounds": fdc.get("totalRewardedVotingRounds"),
                    "epoch_end_time_unix": end_ts,
                    "source_file": f"flare/{epoch}/minimal-conditions.json",
                })
                for feed in feed_rows:
                    feed_hits, total_hits = int(feed["feedHits"]), int(feed["totalHits"])
                    feed_performance.append({
                        "reward_epoch_id": epoch,
                        "voter_address": voter,
                        "data_provider_name": row.get("dataProviderName"),
                        "feed_name": feed["feedName"],
                        "feed_hits": feed_hits,
                        "total_hits": total_hits,
                        "hit_rate_ppm": safe_ratio_ppm(feed_hits, total_hits),
                        "ftso_scaling_condition_met": ftso.get("conditionMet"),
                        "eligible_for_reward": row.get("eligibleForReward"),
                        "source_file": f"flare/{epoch}/minimal-conditions.json",
                    })
            for row in pass_data:
                voter = str(row["voterAddress"]).lower()
                row_failures = row.get("failures") or []
                failure_count += len(row_failures)
                pass_rows.append({
                    "reward_epoch_id": epoch,
                    "data_provider_name": row.get("dataProviderName"),
                    "voter_address": voter,
                    "eligible_for_reward": row["eligibleForReward"],
                    "passes": row["passes"],
                    "failure_count": len(row_failures),
                    "failure_ids": [failure["failureId"] for failure in row_failures],
                    "source_file": f"flare/{epoch}/passes.json",
                })
                for failure_index, failure in enumerate(row_failures):
                    failures.append({
                        "reward_epoch_id": epoch,
                        "voter_address": voter,
                        "data_provider_name": row.get("dataProviderName"),
                        "failure_index": failure_index,
                        "protocol_id": int(failure["protocolId"]),
                        "failure_id": failure["failureId"],
                        "eligible_for_reward": row["eligibleForReward"],
                        "passes_after_epoch": row["passes"],
                        "source_file": f"flare/{epoch}/passes.json",
                    })
        epochs.append({
            "reward_epoch_id": epoch,
            "epoch_start_time_unix": start_ts,
            "epoch_start_time": iso_timestamp(start_ts),
            "epoch_end_time_unix": end_ts,
            "epoch_end_time": iso_timestamp(end_ts),
            "expected_start_voting_round_id": int(info["expectedStartVotingRoundId"]),
            "end_voting_round_id": int(info["endVotingRoundId"]),
            "vote_power_block": int(info["votePowerBlock"]),
            "vote_power_timestamp": int(info["votePowerTimestamp"]),
            "registered_voters": len(registrations_in_epoch),
            "canonical_feeds": len(info.get("canonicalFeedOrder") or []),
            "reward_claims": len(epoch_claims),
            "weight_based_claims": int(distribution["noOfWeightBasedClaims"]),
            "minimum_condition_rows": condition_count,
            "failure_rows": failure_count,
            "applied_minimum_conditions": distribution.get("appliedMinConditions"),
            "published_merkle_root": published_root,
            "onchain_rewards_hash_at_cutoff": root_by_epoch[epoch].lower(),
            "merkle_root_matches_onchain": root_matches,
            "voting_round_bounds_qc": round_qc,
            "weight_based_claim_count_qc": weight_qc,
            "source_commit": source_commit,
        })

    unique_claim_keys = [(row["reward_epoch_id"], row["beneficiary"], row["claim_type_id"]) for row in claims]
    duplicate_claims = len(unique_claim_keys) - len(set(unique_claim_keys))
    beneficiaries = sorted(all_beneficiary_roles)
    chill_calls = [
        [{"to": VOTER_REGISTRY, "data": bytes20_call_data(CHILLED_UNTIL_SELECTOR, beneficiary)}, cutoff_block_hex]
        for beneficiary in beneficiaries
    ]
    chill_results: list[str] = []
    for group in batches(chill_calls):
        chill_results.extend(rpc.batch("eth_call", group))
    chill_state = []
    for beneficiary, encoded in zip(beneficiaries, chill_results, strict=True):
        until_epoch = int(encoded, 16)
        if until_epoch:
            chill_state.append({
                "beneficiary": beneficiary,
                "beneficiary_roles": sorted(all_beneficiary_roles[beneficiary]),
                "chilled_until_reward_epoch_id": until_epoch,
                "active_at_cutoff_epoch": until_epoch >= CUTOFF_ACTIVE_EPOCH,
                "state_block": cutoff_block,
                "source_contract": VOTER_REGISTRY,
            })

    outputs = {
        "flare_reward_epochs": epochs,
        "flare_reward_claims": claims,
        "flare_voter_registrations": registrations,
        "flare_provider_conditions": conditions,
        "flare_provider_feed_performance": feed_performance,
        "flare_provider_passes": pass_rows,
        "flare_provider_failures": failures,
        "flare_beneficiary_chill_state": chill_state,
    }
    row_counts = {name: write_jsonl(curated_dir / f"{name}.jsonl", rows) for name, rows in outputs.items()}
    onchain_evidence = {
        "chain_id": chain_id,
        "cutoff_block": cutoff_block,
        "cutoff_block_hash": cutoff_header["hash"],
        "cutoff_block_timestamp": int(cutoff_header["timestamp"], 16),
        "flare_systems_manager": FLARE_SYSTEMS_MANAGER,
        "voter_registry": VOTER_REGISTRY,
        "rewards_hash_by_epoch": {str(key): value for key, value in root_by_epoch.items()},
        "chill_beneficiaries_queried": len(beneficiaries),
        "nonzero_chill_states": chill_state,
    }
    (raw_dir / "onchain_state_at_cutoff.json").write_text(
        json.dumps(onchain_evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    all_assertions = (
        len(epochs) == LAST_EPOCH - FIRST_EPOCH + 1
        and all(row["epoch_end_time_unix"] <= int(CUTOFF.timestamp()) for row in epochs)
        and all(row["merkle_root_matches_onchain"] for row in epochs)
        and all(row["voting_round_bounds_qc"] for row in epochs)
        and all(row["weight_based_claim_count_qc"] for row in epochs)
        and duplicate_claims == 0
        and condition_mismatches == 0
        and feed_total_mismatches == 0
        and registration_link_misses == 0
    )
    manifest = {
        "dataset": "Flare FSP reward and FTSOv2 provider accountability ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": chain_id,
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_block": cutoff_block,
        "cutoff_block_time": iso_timestamp(int(cutoff_header["timestamp"], 16)),
        "cutoff_block_hash": cutoff_header["hash"],
        "first_reward_epoch": FIRST_EPOCH,
        "last_reward_epoch": LAST_EPOCH,
        "excluded_straddling_epoch": 411,
        "first_minimum_conditions_epoch": FIRST_MINIMUM_CONDITIONS_EPOCH,
        "source_repository": f"https://github.com/{REPOSITORY}",
        "source_commit": source_commit,
        "raw_directory": str(raw_dir),
        "raw_files": len(downloads),
        "raw_bytes": sum(row["bytes"] for row in downloads.values()),
        "raw_sha256": {key: value["sha256"] for key, value in sorted(downloads.items())},
        "row_counts": row_counts,
        "claim_counts_by_type": dict(claim_types),
        "claim_amount_raw_by_type": {key: str(value) for key, value in amount_by_type.items()},
        "duplicate_claim_keys": duplicate_claims,
        "condition_pass_mismatches": condition_mismatches,
        "feed_total_mismatches": feed_total_mismatches,
        "condition_registration_link_misses": registration_link_misses,
        "merkle_roots_matching_onchain_at_cutoff": sum(row["merkle_root_matches_onchain"] for row in epochs),
        "beneficiaries_queried_for_chill_state": len(beneficiaries),
        "nonzero_chill_states": len(chill_state),
        "all_required_assertions_pass": all_assertions,
        "scope_guard": "Merkle claims are aggregate FSP entitlements, not pure FTSO accuracy rewards. Minimum conditions combine FTSO scaling, Fast Updates, staking availability, and FDC. Explicit historical BeneficiaryChilled logs are not bulk-collected because the public RPC limits eth_getLogs to 30 blocks; cutoff contract state is retained instead.",
    }
    if not all_assertions:
        raise RuntimeError(f"Flare QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/flare_fsp_rewards.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = f"""# Flare FSP / FTSOv2 accountability QC

Generated: {manifest['generated_at_utc']}  
Chain: Flare Mainnet (`chainId=14`)  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff block: {cutoff_block} ({manifest['cutoff_block_time']})

## Result

- Complete finalized reward epochs in the official repository through the cutoff: {len(epochs)} (228–410). Epoch 411 is excluded because it ends after the cutoff.
- Official calculation files: {len(downloads)} files / {manifest['raw_bytes'] / 1024 / 1024:.1f} MiB, pinned to commit `{source_commit}` with per-file SHA-256.
- Reward Merkle claims: {len(claims):,}; every one of {len(epochs)} published Merkle roots equals `FlareSystemsManager.rewardsHash(epoch)` at the cutoff block.
- Provider minimum-condition rows: {len(conditions):,}; feed/provider performance rows: {len(feed_performance):,}; pass outcomes: {len(pass_rows):,}; failure reasons: {len(failures):,}.
- Voter registrations: {len(registrations):,}; condition-to-registration misses: {registration_link_misses}.
- Duplicate epoch/beneficiary/claim-type keys: {duplicate_claims}; condition/pass mismatches: {condition_mismatches}; feed aggregate mismatches: {feed_total_mismatches}.
- `VoterRegistry.chilledUntilRewardEpochId` queried for {len(beneficiaries):,} observed beneficiaries at the cutoff; nonzero states: {len(chill_state)}.

## Interpretation guards

- Claim types are the official `DIRECT`, `FEE`, `WNAT`, `MIRROR`, and `CCHAIN` enum. Amounts are native FLR wei.
- The reward Merkle tree aggregates FSP protocols. A claim is therefore labeled an aggregate FSP entitlement, not wholly as a median-accuracy reward.
- Minimum-condition eligibility combines FTSO scaling, Fast Updates, staking availability, and FDC. Feed-hit rows isolate the FTSO scaling evidence.
- FTSO feed hits measure protocol consensus-band alignment, not external objective price truth.
- Pass loss and reward ineligibility are directly observable in official epoch outputs; no unreported monetary forfeiture amount is invented.
- The public RPC limits `eth_getLogs` to 30 blocks. Historical `BeneficiaryChilled` event bulk collection is therefore not claimed; contract chill state at the cutoff is preserved.
"""
    (ROOT / "reports/flare_ftso_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
