"""Flare FSP reward-epoch helpers used by the strict-cutoff adapter."""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import requests


FIRST_VOTING_ROUND_START_TS = 1_658_429_955
VOTING_ROUND_SECONDS = 90
VOTING_ROUNDS_PER_REWARD_EPOCH = 3_360
CLAIM_TYPES = {0: "DIRECT", 1: "FEE", 2: "WNAT", 3: "MIRROR", 4: "CCHAIN"}


def reward_epoch_bounds(reward_epoch_id: int) -> tuple[int, int]:
    start = FIRST_VOTING_ROUND_START_TS + reward_epoch_id * VOTING_ROUNDS_PER_REWARD_EPOCH * VOTING_ROUND_SECONDS
    return start, start + VOTING_ROUNDS_PER_REWARD_EPOCH * VOTING_ROUND_SECONDS


def iso_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")


class FlareRpc:
    """Small retrying JSON-RPC client with batch-call support."""

    def __init__(self, url: str, timeout: int = 60) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        self.request_id = 0

    def _post(self, payload: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == 5:
                    break
                time.sleep(min(2**attempt, 20))
        raise RuntimeError("Flare RPC request failed after retries") from last_error

    def call(self, method: str, params: list[Any]) -> Any:
        self.request_id += 1
        body = self._post({"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params})
        if body.get("error"):
            raise RuntimeError(f"Flare RPC error for {method}: {body['error']}")
        return body["result"]

    def batch(self, method: str, params: list[list[Any]]) -> list[Any]:
        payload = []
        ids: list[int] = []
        for values in params:
            self.request_id += 1
            ids.append(self.request_id)
            payload.append({"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": values})
        rows = self._post(payload)
        by_id = {int(row["id"]): row for row in rows}
        results = []
        for request_id in ids:
            row = by_id[request_id]
            if row.get("error"):
                raise RuntimeError(f"Flare batch RPC error for {method}: {row['error']}")
            results.append(row["result"])
        return results

    def block_at_or_before(self, cutoff_timestamp: int) -> tuple[int, dict[str, Any]]:
        low, high = 0, int(self.call("eth_blockNumber", []), 16)
        while low < high:
            middle = (low + high + 1) // 2
            block = self.call("eth_getBlockByNumber", [hex(middle), False])
            if int(block["timestamp"], 16) <= cutoff_timestamp:
                low = middle
            else:
                high = middle - 1
        return low, self.call("eth_getBlockByNumber", [hex(low), False])


def uint256_call_data(selector: str, value: int) -> str:
    return selector + value.to_bytes(32, "big").hex()


def bytes20_call_data(selector: str, value: str) -> str:
    raw = value.lower().removeprefix("0x")
    if len(raw) != 40:
        raise ValueError(f"expected bytes20 value, got {value}")
    return selector + raw + "0" * 24
