"""Build exact Polygon OOV2 -> ChildTunnel -> Ethereum VotingV2 links."""
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from eth_abi import encode
from eth_utils import keccak

from .polygon_uma import CONTRACTS
from .rpc import write_json


def rows(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def curated_dir(root: Path) -> Path:
    configured = os.environ.get("ORACLE_LEDGER_CURATED_DIR", "data/curated")
    path = Path(configured)
    return path if path.is_absolute() else root / path


def stamp_oov2_ancillary(ancillary_hex: str, requester: str) -> bytes:
    ancillary = bytes.fromhex(ancillary_hex[2:])
    prefix = b"," if ancillary else b""
    return ancillary + prefix + b"ooRequester:" + requester[2:].lower().encode("ascii")


def stamp_legacy_child_ancillary(ancillary: bytes) -> bytes:
    return (
        ancillary
        + b",childRequester:"
        + CONTRACTS["optimistic_oracle_v2"][2:].lower().encode("ascii")
        + b",childChainId:137"
    )


def compress_child_ancillary(ancillary: bytes, child_block: int) -> bytes:
    return (
        b"ancillaryDataHash:" + keccak(ancillary).hex().encode("ascii")
        + b",childBlockNumber:" + str(child_block).encode("ascii")
        + b",childOracle:" + CONTRACTS["oracle_child_tunnel"][2:].lower().encode("ascii")
        + b",childRequester:" + CONTRACTS["optimistic_oracle_v2"][2:].lower().encode("ascii")
        + b",childChainId:137"
    )


def oracle_request_hash(identifier: str, timestamp: int, ancillary: bytes) -> str:
    return "0x" + keccak(encode(["bytes32", "uint256", "bytes"], [bytes.fromhex(identifier[2:]), timestamp, ancillary])).hex()


def build(root: Path) -> Path:
    root = root.resolve(); curated = curated_dir(root)
    child_path = curated / "polygon_child_tunnel_events.jsonl"
    rounds_path = curated / "polygon_uma_request_rounds.jsonl"
    dvm_path = curated / "uma_dvm_requests.jsonl"
    for path in (child_path, rounds_path, dvm_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    bridges_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    price_added: dict[str, list[dict[str, Any]]] = defaultdict(list)
    price_added_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pushed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    resolved_legacy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows(child_path):
        if row["event"] == "PriceRequestBridged":
            bridges_by_tx[row["source_tx"]].append(row)
        elif row["event"] == "PriceRequestAdded":
            price_added[row["request_hash"]].append(row)
            price_added_by_tx[row["source_tx"]].append(row)
        elif row["event"] == "PushedPrice":
            pushed[row["request_hash"]].append(row)
        elif row["event"] == "ResolvedLegacyRequest":
            resolved_legacy[row["request_hash"]].append(row)
            resolved_legacy[row["legacy_request_hash"]].append(row)

    dvm_exact: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows(dvm_path):
        dvm_exact[(row["identifier"], row["request_time"], row["ancillary_data_hex"])].append(row)

    output = curated / "uma_polygon_ethereum_grade_a_links.jsonl"
    temporary = output.with_suffix(output.suffix + ".tmp")
    counts: Counter[str] = Counter(); ambiguous = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for request in rows(rounds_path):
            if request.get("disputer", "0x" + "00" * 20) == "0x" + "00" * 20:
                continue
            stamped = stamp_oov2_ancillary(request["ancillary_data_hex"], request["requester"])
            # Event-based OOV2 requests use proposal time, not the original OO
            # request time, when escalating to the DVM. The bridge event is
            # emitted synchronously inside the same DisputePrice transaction.
            candidates = [
                row for row in bridges_by_tx.get(request.get("dispute_tx", ""), [])
                if row["identifier"] == request["identifier"]
                and row["requester"] == CONTRACTS["optimistic_oracle_v2"].lower()
            ]
            link: dict[str, Any] = {
                "oo_request_id": request["oo_request_id"], "question_id": request.get("question_id"),
                "identifier": request["identifier"], "request_time": request["request_time"],
                "sample_tier": request.get("sample_tier"),
                "cross_chain_match_grade": "U",
            }
            if len(candidates) == 1:
                bridge = candidates[0]
                computed_child_hash = oracle_request_hash(request["identifier"], int(bridge["dvm_time"]), stamped)
                link.update(
                    child_bridge_tx=bridge["source_tx"], child_bridge_block=bridge["source_block"],
                    parent_request_hash=bridge["parent_request_hash"], child_request_hash=bridge["child_request_hash"],
                    computed_child_request_hash=computed_child_hash,
                    dvm_time=bridge["dvm_time"],
                    stamped_ancillary_exact=(bridge["child_ancillary_data_hex"] == "0x" + stamped.hex()),
                    child_request_hash_exact=(bridge["child_request_hash"] == computed_child_hash),
                )
                parent_events = price_added.get(bridge["parent_request_hash"], [])
                if len(parent_events) == 1 and link["stamped_ancillary_exact"] and link["child_request_hash_exact"]:
                    parent = parent_events[0]
                    dvm_candidates = dvm_exact.get(
                        (parent["identifier"], parent["dvm_time"], parent["stamped_ancillary_data_hex"]), []
                    )
                    if len(dvm_candidates) == 1:
                        dvm = dvm_candidates[0]
                        price_events = (
                            pushed.get(bridge["child_request_hash"], [])
                            or pushed.get(bridge["parent_request_hash"], [])
                            or resolved_legacy.get(bridge["child_request_hash"], [])
                            or resolved_legacy.get(bridge["parent_request_hash"], [])
                        )
                        pushed_price = price_events[-1].get("resolved_price_raw") if price_events else None
                        dvm_price = dvm.get("resolved_price_raw"); oo_price = request.get("resolved_price_raw")
                        three_way = dvm_price == pushed_price == oo_price if None not in {dvm_price, pushed_price, oo_price} else None
                        link.update(
                            dvm_request_id=dvm["dvm_request_id"], dvm_status=dvm.get("status"),
                            dvm_resolved_price_raw=dvm_price, child_pushed_price_raw=pushed_price,
                            oo_settled_price_raw=oo_price, resolved_price_consistent=three_way,
                            cross_chain_match_grade="A",
                        )
                    elif len(dvm_candidates) > 1:
                        ambiguous += 1
                elif len(parent_events) > 1:
                    ambiguous += 1
            elif len(candidates) > 1:
                ambiguous += 1
            else:
                # Before PriceRequestBridged was introduced, the legacy child
                # tunnel emitted PriceRequestAdded synchronously in the same
                # dispute transaction after appending childRequester/chainId.
                legacy_candidates = [
                    row for row in price_added_by_tx.get(request.get("dispute_tx", ""), [])
                    if row["identifier"] == request["identifier"]
                ]
                if len(legacy_candidates) == 1:
                    parent = legacy_candidates[0]
                    variants = [
                        ("compressed_pre_bridge_event", compress_child_ancillary(stamped, parent["source_block"])),
                        ("legacy_stamped", stamp_legacy_child_ancillary(stamped)),
                    ]
                    matched_variant = next(
                        ((name, value) for name, value in variants if parent["stamped_ancillary_data_hex"] == "0x" + value.hex()),
                        None,
                    )
                    matched_ancillary = matched_variant[1] if matched_variant else b""
                    computed = oracle_request_hash(request["identifier"], int(parent["dvm_time"]), matched_ancillary) if matched_variant else ""
                    exact = matched_variant is not None and parent["request_hash"] == computed
                    if exact:
                        link.update(
                            child_bridge_tx=parent["source_tx"], child_bridge_block=parent["source_block"],
                            parent_request_hash=parent["request_hash"], child_request_hash=parent["request_hash"],
                            computed_child_request_hash=computed, dvm_time=parent["dvm_time"],
                            stamped_ancillary_exact=True, child_request_hash_exact=True,
                            child_tunnel_link_mode=matched_variant[0],
                        )
                        dvm_candidates = dvm_exact.get(
                            (parent["identifier"], parent["dvm_time"], parent["stamped_ancillary_data_hex"]), []
                        )
                        if len(dvm_candidates) == 1:
                            dvm = dvm_candidates[0]; price_events = pushed.get(parent["request_hash"], [])
                            pushed_price = price_events[-1].get("resolved_price_raw") if price_events else None
                            dvm_price = dvm.get("resolved_price_raw"); oo_price = request.get("resolved_price_raw")
                            three_way = dvm_price == pushed_price == oo_price if None not in {dvm_price, pushed_price, oo_price} else None
                            link.update(
                                dvm_request_id=dvm["dvm_request_id"], dvm_status=dvm.get("status"),
                                dvm_resolved_price_raw=dvm_price, child_pushed_price_raw=pushed_price,
                                oo_settled_price_raw=oo_price, resolved_price_consistent=three_way,
                                cross_chain_match_grade="A",
                            )
                        elif len(dvm_candidates) == 0:
                            link["unmatched_reason"] = "exact_polygon_bridge_found_but_no_votingv2_request"
                        elif len(dvm_candidates) > 1:
                            ambiguous += 1
                elif len(legacy_candidates) > 1:
                    ambiguous += 1
            counts[link["cross_chain_match_grade"]] += 1
            handle.write(json.dumps(link, separators=(",", ":")) + "\n")
    temporary.replace(output)
    manifest = {
        "protocol": "Polymarket UMA Polygon-Ethereum linkage", "disputed_oov2_requests": sum(counts.values()),
        "by_grade": dict(sorted(counts.items())), "ambiguous_matches": ambiguous,
        "grade_a_policy": "exact childRequestId + parentRequestId + identifier/time/compressed ancillary match",
        "output": str(output),
    }
    manifest_path = root / "data/manifests/uma_crosschain_links.json"
    write_json(manifest_path, manifest)
    return manifest_path
