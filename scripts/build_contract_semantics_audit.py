#!/usr/bin/env python3
"""Freeze source evidence and build function-level reward/slash semantics rules.

This audit deliberately distinguishes source semantics from event names.  Ethereum
contracts use explorer-verified deployed source where it is available.  Other
protocols use a fixed official repository commit and are graded below A until the
deployed binary can be reproduced from that source.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from eth_utils import keccak


ROOT = Path(__file__).resolve().parents[1]
RAW = (ROOT / "data/raw/source_audit").resolve()
MANIFEST = ROOT / "data/manifests/contract_semantics_audit.json"
REPORT = ROOT / "reports/contract_semantics_audit.md"

UMA_COMMIT = "a16ee53125c433dfa4e29738b73d9069ff109c03"
TELLOR_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
TELLOR_LEGACY_REWARD_COMMIT = "3797b83a4222e74df0154f9c23ea118539882fde"
FLARE_COMMIT = "62aca6a6ec0fa59b784526c67f40883e787aba96"
PYTH_COMMIT = "68a9a36ec3d41364490e71b056b422c99f13e0cf"
CHRONICLE_COMMIT = "12ff06ca78811e01313afde4b38fe959d6647096"


def raw(repo: str, commit: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{repo}/{commit}/{path}"


SOURCE_SPECS: list[dict[str, Any]] = [
    {
        "id": "uma_oov2",
        "protocol": "UMA",
        "kind": "github",
        "repository": "https://github.com/UMAprotocol/protocol",
        "commit": UMA_COMMIT,
        "path": "packages/core/contracts/optimistic-oracle-v2/implementation/OptimisticOracleV2.sol",
        "url": raw("UMAprotocol/protocol", UMA_COMMIT, "packages/core/contracts/optimistic-oracle-v2/implementation/OptimisticOracleV2.sol"),
        "deployed_address": "0xee3afe347d5c74317041e2618c49534daf887c24",
        "runtime_code_hash": "0xdc59e0d28ac7f24147f6a9e24e8e7a011ba1575a6ec5ec7d73f1c87666927856",
        "source_confidence": "B",
        "confidence_reason": "fixed official source and cutoff runtime hash; compiler-level source/runtime reproduction is not archived",
    },
    {
        "id": "uma_voting_v2",
        "protocol": "UMA",
        "kind": "blockscout",
        "url": "https://eth.blockscout.com/api/v2/smart-contracts/0x004395edb43EFca9885CEdad51EC9fAf93Bd34ac",
        "deployed_address": "0x004395edb43efca9885cedad51ec9faf93bd34ac",
        "runtime_code_hash": "0x1b8bf8377b8f2ac8b465146937427cd4f1942484a8d02b680c16dcca4c139785",
        "source_confidence": "A",
        "confidence_reason": "deployed Ethereum address has verified source and an independently recorded cutoff runtime hash",
    },
    {
        "id": "chainlink_reward_vault",
        "protocol": "Chainlink",
        "kind": "blockscout",
        "url": "https://eth.blockscout.com/api/v2/smart-contracts/0x996913c8c08472f584ab8834e925b06d0eb1d813",
        "deployed_address": "0x996913c8c08472f584ab8834e925b06d0eb1d813",
        "runtime_code_hash": "0x58f5788dc9004e3aacbaa3afd5f3f5806227fbbb1ace4a03183c80ae6904c93c",
        "source_confidence": "A",
        "confidence_reason": "deployed Ethereum address has verified source and an independently recorded cutoff runtime hash",
    },
    {
        "id": "chainlink_operator_pool",
        "protocol": "Chainlink",
        "kind": "blockscout",
        "url": "https://eth.blockscout.com/api/v2/smart-contracts/0xa1d76a7ca72128541e9fcacafbda3a92ef94fdc5",
        "deployed_address": "0xa1d76a7ca72128541e9fcacafbda3a92ef94fdc5",
        "runtime_code_hash": "0x7e69984d9b53291326e8d67c4e6600088af546959641df789f1759d1ea3b089c",
        "source_confidence": "A",
        "confidence_reason": "deployed Ethereum address has verified source and an independently recorded cutoff runtime hash",
    },
    {
        "id": "chainlink_alert_controller",
        "protocol": "Chainlink",
        "kind": "blockscout",
        "url": "https://eth.blockscout.com/api/v2/smart-contracts/0x27484ba119d12649be2a9854e4d3b44cc3fdbad7",
        "deployed_address": "0x27484ba119d12649be2a9854e4d3b44cc3fdbad7",
        "runtime_code_hash": "0xf6e759f5d591ab81492928df2c3d9a910b2628a0f79f8b77ef1bd79ae1757e78",
        "source_confidence": "A",
        "confidence_reason": "deployed Ethereum address has verified source and an independently recorded cutoff runtime hash",
    },
    {
        "id": "tellor_claim_reward",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/dispute/keeper/claim_reward.go",
        "url": raw("tellor-io/layer", TELLOR_COMMIT, "x/dispute/keeper/claim_reward.go"),
        "source_confidence": "B",
        "confidence_reason": "fixed official cutoff source; the queried mainnet node did not expose its application git commit",
    },
    {
        "id": "tellor_dispute",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/dispute/keeper/dispute.go",
        "url": raw("tellor-io/layer", TELLOR_COMMIT, "x/dispute/keeper/dispute.go"),
        "source_confidence": "B",
        "confidence_reason": "fixed official cutoff source; the queried mainnet node did not expose its application git commit",
    },
    {
        "id": "tellor_execute_dispute",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/dispute/keeper/execute.go",
        "url": raw("tellor-io/layer", TELLOR_COMMIT, "x/dispute/keeper/execute.go"),
        "source_confidence": "B",
        "confidence_reason": "fixed official cutoff source; the queried mainnet node did not expose its application git commit",
    },
    {
        "id": "tellor_reporter_distribution",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/reporter/keeper/distribution.go",
        "url": raw("tellor-io/layer", TELLOR_COMMIT, "x/reporter/keeper/distribution.go"),
        "source_confidence": "B",
        "confidence_reason": "fixed official cutoff source; reward accrual and payment remain separately classified",
    },
    {
        "id": "tellor_legacy_reporter_distribution",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_LEGACY_REWARD_COMMIT,
        "path": "x/reporter/keeper/distribution.go",
        "url": raw(
            "tellor-io/layer",
            TELLOR_LEGACY_REWARD_COMMIT,
            "x/reporter/keeper/distribution.go",
        ),
        "source_confidence": "B",
        "confidence_reason": (
            "fixed official pre-v6.1 source; the deployed rewards_added event "
            "contains only the post-update cumulative amount and a raw-byte "
            "selector value"
        ),
    },
    {
        "id": "tellor_tip_withdrawal",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/reporter/keeper/msg_server.go",
        "url": raw("tellor-io/layer", TELLOR_COMMIT, "x/reporter/keeper/msg_server.go"),
        "source_confidence": "B",
        "confidence_reason": "fixed official cutoff source plus transaction-level escrow coin-spent evidence",
    },
    {
        "id": "tellor_submit_value",
        "protocol": "Tellor",
        "kind": "github",
        "repository": "https://github.com/tellor-io/layer",
        "commit": TELLOR_COMMIT,
        "path": "x/oracle/keeper/submit_value.go",
        "url": raw(
            "tellor-io/layer",
            TELLOR_COMMIT,
            "x/oracle/keeper/submit_value.go",
        ),
        "source_confidence": "B",
        "confidence_reason": (
            "fixed official cutoff source plus immutable successful-transaction "
            "new_report events from every canonical block"
        ),
    },
    {
        "id": "flare_reward_manager",
        "protocol": "Flare",
        "kind": "github",
        "repository": "https://github.com/flare-foundation/flare-smart-contracts-v2",
        "commit": FLARE_COMMIT,
        "path": "contracts/protocol/implementation/RewardManager.sol",
        "url": raw("flare-foundation/flare-smart-contracts-v2", FLARE_COMMIT, "contracts/protocol/implementation/RewardManager.sol"),
        "source_confidence": "B",
        "confidence_reason": "official fixed source plus on-chain claim-event and epoch-total reconciliation",
    },
    {
        "id": "pyth_integrity_pool",
        "protocol": "Pyth",
        "kind": "github",
        "repository": "https://github.com/pyth-network/governance",
        "commit": PYTH_COMMIT,
        "path": "staking/programs/integrity-pool/src/lib.rs",
        "url": raw("pyth-network/governance", PYTH_COMMIT, "staking/programs/integrity-pool/src/lib.rs"),
        "deployed_address": "pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN",
        "source_confidence": "B",
        "confidence_reason": "official fixed program source and on-chain account state; deployed program binary was not reproduced",
    },
    {
        "id": "chronicle_scribe_optimistic",
        "protocol": "Chronicle",
        "kind": "github",
        "repository": "https://github.com/chronicleprotocol/scribe",
        "commit": CHRONICLE_COMMIT,
        "path": "src/ScribeOptimistic.sol",
        "url": raw("chronicleprotocol/scribe", CHRONICLE_COMMIT, "src/ScribeOptimistic.sol"),
        "source_confidence": "B",
        "confidence_reason": "fixed official source plus deployed Scribe interface validation and Ethereum event evidence",
    },
    {
        "id": "dia_external_staking",
        "protocol": "DIA",
        "kind": "blockscout",
        "url": "https://explorer.diadata.org/api/v2/smart-contracts/0x677Cf1299c367F6cf6F3E1669aCC18Fd059a5919",
        "deployed_address": "0x677cf1299c367f6cf6f3e1669acc18fd059a5919",
        "runtime_code_hash": "0x23edc1bb93507ca16136417ed6f9729129c42bf22579d57dbf9f9b29b831450e",
        "source_confidence": "A",
        "confidence_reason": "Lasernet explorer-verified deployed source and cutoff-block runtime hash match",
    },
]


RULE_SPECS = [
    ("UMA_OOV2_SETTLEMENT_PAYMENT_V1", "uma_oov2", "safeTransfer(disputeSuccess ? request.disputer : request.proposer, payout)", "paid_reward_embedded_in_settlement", "gross payout is transferred to the winner; reward is the source-defined non-principal component"),
    ("UMA_OOV2_BOND_FORFEITURE_V1", "uma_oov2", "uint256 unburnedBond = bond.sub(_computeBurnedBond(request))", "realized_bond_forfeiture", "the winner receives the unburned part of the loser's bond and the burned part is paid as oracle fees"),
    ("UMA_DVM_VOTER_SLASH_ACCRUAL_V1", "uma_voting_v2", "voterStake.unappliedSlash += int128(slash)", "calculated_stake_delta", "VoterSlashed records a request-level accrual, not the final stake mutation"),
    ("UMA_DVM_VOTER_SLASH_APPLIED_V1", "uma_voting_v2", "emit VoterSlashApplied(voter, voterStake.unappliedSlash, voterStake.stake)", "applied_net_stake_delta", "the stake has already been mutated and postStake is emitted; do not add VoterSlashed again"),
    ("CHAINLINK_REWARD_CLAIM_PAYMENT_V1", "chainlink_reward_vault", "i_LINK.transfer(msg.sender, newVestedRewards)", "paid_reward", "RewardClaimed follows an ERC-20 LINK transfer of the same computed amount"),
    ("CHAINLINK_FORFEITURE_ACCOUNTING_V1", "chainlink_reward_vault", "emit ForfeitedRewardDistributed", "accounting_redistribution", "unvested rewards are redistributed through reward-per-token accounting; no principal transfer occurs here"),
    ("CHAINLINK_OPERATOR_PRINCIPAL_SLASH_V1", "chainlink_operator_pool", "emit Slashed", "realized_principal_slash", "operator principal and total principal are reduced before Slashed is emitted"),
    ("CHAINLINK_ALERT_CONFIG_PARAMETER_V1", "chainlink_alert_controller", "config.slashableAmount = configParams.slashableAmount", "designed_slash_and_reward_parameter", "FeedConfigSet stores the per-feed slashable principal and alerter reward amounts; configuration is not execution"),
    ("CHAINLINK_ALERT_TRIGGER_V1", "chainlink_alert_controller", "slashAndReward", "trigger_not_payment", "AlertRaised initiates slashing/reward logic but is not itself proof that LINK was paid"),
    ("TELLOR_VOTER_REWARD_PAYMENT_V1", "tellor_claim_reward", "SendCoinsFromModuleToAccount", "paid_reward", "the dispute module sends loya to the claimant"),
    ("TELLOR_REPORTER_STAKE_ESCROW_V1", "tellor_dispute", "EscrowReporterStake", "escrowed_stake", "stake is escrowed when a funded dispute starts; final slash depends on execution outcome"),
    ("TELLOR_SLASH_CATEGORY_PARAMETER_V1", "tellor_dispute", "return math.NewInt(layertypes.PowerReduction.Int64()).QuoRaw(100)", "designed_slash_parameter", "category parameters are Warning 1%, Minor 5%, and Major 100%; transaction state supplies the actual slash amount"),
    ("TELLOR_REPORTER_SLASH_FINAL_V1", "tellor_execute_dispute", "VoteResult_SUPPORT", "realized_principal_slash", "support outcomes retain the reporter stake; against/invalid outcomes return it"),
    ("TELLOR_REWARD_ACCRUAL_V1", "tellor_reporter_distribution", "periodData.RewardAmount = periodData.RewardAmount.Add(netReward)", "accrued_reward_not_payment", "the reporter-period balance is updated, but no account payment occurs here"),
    ("TELLOR_LEGACY_REWARDS_ADDED_CUMULATIVE_ONLY_V1", "tellor_legacy_reporter_distribution", 'sdk.NewAttribute("amount", newTips.String())', "cumulative_reward_balance_increment_unobservable", "the deployed legacy event exposes the post-update balance but neither a canonical selector address nor the per-event increment"),
    ("TELLOR_TIP_WITHDRAWAL_PAYMENT_V1", "tellor_tip_withdrawal", "DelegateCoinsFromAccountToModule", "paid_reward_compounded_to_stake", "the selector's settled tip balance is moved from tips escrow into bonded stake"),
    ("TELLOR_NEW_REPORT_BLOCK_EVENT_V1", "tellor_submit_value", "k.Reports.Set(ctx, collections.Join3(queryId, reporter.Bytes(), query.Id), report)", "immutable_report_storage", "a successful new_report transaction stores one report under the exact (query_id, reporter, query-meta id) keeper key"),
    ("FLARE_MERKLE_ENTITLEMENT_V1", "flare_reward_manager", "merkle proof invalid", "claimable_entitlement", "a valid Merkle leaf establishes entitlement only; _claim and transfer are separate actions"),
    ("FLARE_REWARD_PAYMENT_V1", "flare_reward_manager", "function _transferOrWrap", "paid_reward", "payment requires execution of claim and a transfer/wrap; the current Merkle dataset does not prove this"),
    ("PYTH_REWARD_FACTOR_PARAMETER_V1", "pyth_integrity_pool", "calculate_reward", "reward_parameter", "rates/factors feed the calculation but are not token payments"),
    ("PYTH_REWARD_PAYMENT_V1", "pyth_integrity_pool", "anchor_spl::token::transfer(transfer_ctx, delegator_reward)", "paid_reward", "a realized reward requires this token transfer or equivalent transaction evidence"),
    ("PYTH_SLASH_APPLIED_V1", "pyth_integrity_pool", "staking::cpi::slash_account", "realized_principal_slash", "a created slash event is only a parameter; slash_account is the stake mutation"),
    ("CHRONICLE_OP_CHALLENGE_PAYMENT_V1", "chronicle_scribe_optimistic", "if (_sendETH(payable(msg.sender), reward))", "paid_challenge_reward", "OpChallengeRewardPaid is emitted only after the ETH send succeeds"),
    ("DIA_LASERNET_UNSTAKE_REWARD_DECOMPOSITION_V1", "dia_external_staking", "currentStore.requestedUnstakePrincipalRewardAmount", "paid_staking_reward", "unstake transfers the stored principal-wallet and beneficiary reward fields separately from returned principal"),
]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def fetch_source(session: requests.Session, spec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    response = session.get(spec["url"], timeout=60)
    response.raise_for_status()
    if spec["kind"] == "github":
        return response.text, {"source_path": spec["path"], "http_url": spec["url"]}
    payload = response.json()
    if not payload.get("is_verified") or not payload.get("source_code"):
        raise RuntimeError(f"deployed source is not verified: {spec['id']}")
    explorer_runtime_hash = "0x" + keccak(hexstr=payload["deployed_bytecode"]).hex()
    if explorer_runtime_hash.lower() != spec["runtime_code_hash"].lower():
        raise RuntimeError(
            f"verified-source runtime differs from local cutoff runtime for {spec['id']}: "
            f"{explorer_runtime_hash} != {spec['runtime_code_hash']}"
        )
    metadata = {
        "source_path": payload.get("file_path"),
        "contract_name": payload.get("name"),
        "compiler_version": payload.get("compiler_version"),
        "verified_at": payload.get("verified_at"),
        "is_fully_verified": payload.get("is_fully_verified"),
        "explorer_runtime_code_hash": explorer_runtime_hash,
        "runtime_hash_matches_local_cutoff": True,
        "http_url": spec["url"],
    }
    return payload["source_code"], metadata


def locate(text: str, marker: str) -> dict[str, Any]:
    lines = text.splitlines()
    matches = [i for i, line in enumerate(lines, 1) if marker in line]
    if not matches:
        raise RuntimeError(f"semantic source marker not found: {marker}")
    line = matches[0]
    return {
        "line": line,
        "marker": marker,
        "context": "\n".join(f"{i}: {lines[i - 1].strip()}" for i in range(max(1, line - 2), min(len(lines), line + 2) + 1)),
    }


def build() -> dict[str, Any]:
    RAW.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "oracle-nature-source-semantics-audit/1.0"
    source_rows: list[dict[str, Any]] = []
    texts: dict[str, str] = {}
    for spec in SOURCE_SPECS:
        text, fetched = fetch_source(session, spec)
        suffix = Path(fetched["source_path"] or "source.txt").suffix or ".txt"
        output = RAW / f"{spec['id']}{suffix}"
        output.write_text(text, encoding="utf-8")
        texts[spec["id"]] = text
        row = {k: v for k, v in spec.items() if k not in {"url", "kind"}}
        row.update(fetched)
        row.update({"sha256": sha256_text(text), "bytes": len(text.encode()), "local_path": str(output)})
        source_rows.append(row)

    by_id = {row["id"]: row for row in source_rows}
    rules: list[dict[str, Any]] = []
    for rule_id, source_id, marker, semantic_class, conclusion in RULE_SPECS:
        rule = {
            "rule_id": rule_id,
            "source_id": source_id,
            "protocol": by_id[source_id]["protocol"],
            "semantic_class": semantic_class,
            "conclusion": conclusion,
            "source_confidence": by_id[source_id]["source_confidence"],
        }
        rule.update(locate(texts[source_id], marker))
        rules.append(rule)

    manifest = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "method": "function-level source semantics; event names alone are insufficient",
        "sources": source_rows,
        "rules": rules,
        "important_guards": [
            "VoterSlashed is request-level accrual; VoterSlashApplied is the realized net stake mutation and the two must never be summed.",
            "RewardClaimed is paid only when the corresponding token/bank transfer is observed.",
            "Merkle leaves, reward factors, alert configuration, and slash-event parameters are not realized cash flows.",
        ],
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def write_report(manifest: dict[str, Any]) -> None:
    source_lines = [
        f"| {s['protocol']} | `{s['id']}` | {s['source_confidence']} | `{s['sha256']}` | {s['confidence_reason']} |"
        for s in manifest["sources"]
    ]
    rule_lines = [
        f"| `{r['rule_id']}` | {r['semantic_class']} | `{r['source_id']}:{r['line']}` | {r['conclusion']} |"
        for r in manifest["rules"]
    ]
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(
        "# Contract semantics audit\n\n"
        "This is the source-side gate for classifying real rewards and slashes. Event labels are not accepted without the corresponding balance-changing function.\n\n"
        "## Fixed sources\n\n| Protocol | Source id | Grade | SHA-256 | Scope |\n|---|---|---:|---|---|\n"
        + "\n".join(source_lines)
        + "\n\n## Function-level rules\n\n| Rule | Class | Anchor | Meaning |\n|---|---|---|---|\n"
        + "\n".join(rule_lines)
        + "\n\n## Counting guard\n\n`VoterSlashed` is an accrual and `VoterSlashApplied` is the final net stake mutation. Only the latter enters the strict realized table; they are never added together. Flare Merkle leaves and Pyth factors stay outside paid-reward totals until claim/transfer transactions are collected.\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    manifest = build()
    write_report(manifest)
    print(json.dumps({"sources": len(manifest["sources"]), "rules": len(manifest["rules"]), "manifest": str(MANIFEST), "raw_dir": str(RAW)}, indent=2))


if __name__ == "__main__":
    main()
