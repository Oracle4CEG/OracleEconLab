"""Parse Tellor Layer end-block reporting-reward events."""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def event_attributes(event: dict[str, Any]) -> dict[str, str]:
    return {str(row["key"]): str(row["value"]) for row in event.get("attributes") or []}


def parse_liveness_reward_block(
    height: int,
    block_time: str,
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return distribution rows and classified reporter accrual rows.

    ``DistributeLivenessRewards`` emits one ``rewards_accumulated`` event per
    rewarded reporter, then bank-transfer events, then one
    ``liveness_rewards_distributed`` event.  Tip accrual events can appear
    earlier in the same end-block result.  The distribution's reporter_count
    therefore identifies the last N reward-accrual events before it.
    """
    distributions: list[dict[str, Any]] = []
    accruals: list[dict[str, Any]] = []
    cursor = 0
    distribution_index = 0
    while cursor < len(events):
        if events[cursor].get("type") != "liveness_rewards_distributed":
            cursor += 1
            continue
        attrs = event_attributes(events[cursor])
        reporter_count = int(attrs["reporter_count"])
        prior_reward_indexes = [
            index
            for index in range(cursor)
            if events[index].get("type") == "rewards_accumulated"
            and not any(row["event_index"] == index for row in accruals)
        ]
        if len(prior_reward_indexes) < reporter_count:
            raise ValueError(
                f"height {height}: liveness reporter_count={reporter_count}, "
                f"but only {len(prior_reward_indexes)} unclassified accrual events precede it"
            )
        liveness_indexes = set(prior_reward_indexes[-reporter_count:]) if reporter_count else set()
        for index in prior_reward_indexes:
            reward = event_attributes(events[index])
            commission = Decimal(reward["commission"])
            net_reward = Decimal(reward["net_reward"])
            accruals.append({
                "height": height,
                "block_time": block_time,
                "event_index": index,
                "distribution_index": distribution_index if index in liveness_indexes else None,
                "reward_source": "liveness_tbr" if index in liveness_indexes else "query_tip",
                "reporter": reward["reporter"],
                "commission_loya_decimal": str(commission),
                "net_reward_loya_decimal": str(net_reward),
                "gross_reward_loya_decimal": str(commission + net_reward),
                "period_total_loya_decimal": reward["period_total"],
                "semantic_class": "accrued_reward_not_account_payment",
                "rule_id": "TELLOR_REWARDS_ACCUMULATED_V1",
            })
        distributions.append({
            "height": height,
            "block_time": block_time,
            "distribution_index": distribution_index,
            "total_distributed_loya_raw": attrs["total_distributed"],
            "reporter_count": reporter_count,
            "standard_opportunities": int(attrs["standard_opportunities"]),
            "non_standard_queries": int(attrs["non_standard_queries"]),
            "semantic_class": "module_transfer_to_tips_escrow",
            "rule_id": "TELLOR_LIVENESS_REWARDS_DISTRIBUTED_V1",
        })
        distribution_index += 1
        cursor += 1

    # A queried liveness block must contain its marker.
    if not distributions:
        raise ValueError(f"height {height}: no liveness_rewards_distributed event")
    # Capture any tip accruals after the final marker defensively.
    classified = {row["event_index"] for row in accruals}
    for index, event in enumerate(events):
        if event.get("type") != "rewards_accumulated" or index in classified:
            continue
        reward = event_attributes(event)
        commission = Decimal(reward["commission"])
        net_reward = Decimal(reward["net_reward"])
        accruals.append({
            "height": height,
            "block_time": block_time,
            "event_index": index,
            "distribution_index": None,
            "reward_source": "query_tip",
            "reporter": reward["reporter"],
            "commission_loya_decimal": str(commission),
            "net_reward_loya_decimal": str(net_reward),
            "gross_reward_loya_decimal": str(commission + net_reward),
            "period_total_loya_decimal": reward["period_total"],
            "semantic_class": "accrued_reward_not_account_payment",
            "rule_id": "TELLOR_REWARDS_ACCUMULATED_V1",
        })
    accruals.sort(key=lambda row: row["event_index"])
    return distributions, accruals


def parse_full_reward_block(
    height: int,
    block_time: str,
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse legacy selector rewards and current reporter-period accruals.

    Before the liveness-distribution refactor, the deployed
    ``rewards_added`` event exposed only the post-update cumulative selector
    balance as ``amount``.  A source change later added ``rewards_report``, but
    that change was bundled with the upgrade that replaced this event with
    ``rewards_accumulated``; consequently historical mainnet rows do not expose
    an incremental selector reward.  Preserve that observability boundary
    instead of differencing lossy raw-byte selector identifiers or inventing an
    amount.

    Current code emits ``rewards_accumulated`` at reporter-period granularity.
    A current block can also contain a liveness marker; in that case the final N
    accruals before the marker are liveness rewards and the others are query-tip
    rewards.
    """
    legacy: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if event.get("type") != "rewards_added":
            continue
        reward = event_attributes(event)
        incremental = reward.get("rewards_report")
        legacy.append(
            {
                "height": height,
                "block_time": block_time,
                "event_index": index,
                "reward_source": "query_tip",
                # The deployed legacy source emitted string(raw 20-byte
                # address). CometBFT's JSON representation can therefore be
                # invalid UTF-8 and lossy. Retain the source value but do not
                # label it as a canonical Tellor address.
                "selector_event_value_utf8_lossy": reward.get("delegator"),
                "selector_address_observable": bool(
                    str(reward.get("delegator") or "").startswith("tellor1")
                ),
                "incremental_reward_loya_raw": incremental,
                "incremental_reward_observable": incremental is not None,
                "cumulative_selector_tips_loya_decimal": reward["amount"],
                "semantic_class": (
                    "accrued_reward_not_account_payment"
                    if incremental is not None
                    else "cumulative_reward_balance_increment_unobservable"
                ),
                "rule_id": (
                    "TELLOR_LEGACY_REWARDS_ADDED_V1"
                    if incremental is not None
                    else "TELLOR_LEGACY_REWARDS_ADDED_CUMULATIVE_ONLY_V1"
                ),
            }
        )

    if any(event.get("type") == "liveness_rewards_distributed" for event in events):
        distributions, current = parse_liveness_reward_block(height, block_time, events)
    else:
        distributions = []
        current = []
        for index, event in enumerate(events):
            if event.get("type") != "rewards_accumulated":
                continue
            reward = event_attributes(event)
            commission = Decimal(reward["commission"])
            net_reward = Decimal(reward["net_reward"])
            current.append(
                {
                    "height": height,
                    "block_time": block_time,
                    "event_index": index,
                    "distribution_index": None,
                    "reward_source": "query_tip",
                    "reporter": reward["reporter"],
                    "commission_loya_decimal": str(commission),
                    "net_reward_loya_decimal": str(net_reward),
                    "gross_reward_loya_decimal": str(commission + net_reward),
                    "period_total_loya_decimal": reward["period_total"],
                    "semantic_class": "accrued_reward_not_account_payment",
                    "rule_id": "TELLOR_REWARDS_ACCUMULATED_V1",
                }
            )
    return distributions, current, legacy
