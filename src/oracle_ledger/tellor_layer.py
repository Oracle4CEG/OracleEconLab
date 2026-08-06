"""Tellor Layer REST/RPC helpers for dispute-accountability collection."""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import requests


class TellorClient:
    def __init__(self, rpc_url: str, api_url: str, timeout: int = 60) -> None:
        self.rpc_url = rpc_url.rstrip("/")
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"Tellor request failed after retries: {url}") from last_error

    def rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        body = self._request("POST", self.rpc_url, json=payload).json()
        if body.get("error"):
            raise RuntimeError(f"Tellor RPC error for {method}: {body['error']}")
        return body["result"]

    def api(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        return self._request("GET", self.api_url + path, params=params).json()

    def block_time(self, height: int) -> datetime:
        result = self.rpc("block", {"height": str(height)})
        return datetime.fromisoformat(result["block"]["header"]["time"].replace("Z", "+00:00")).astimezone(UTC)

    def latest_height(self) -> int:
        return int(self.rpc("status", {})["sync_info"]["latest_block_height"])

    def height_at_or_before(self, cutoff: datetime) -> int:
        low, high = 1, self.latest_height()
        if self.block_time(low) > cutoff:
            return 0
        while low < high:
            middle = (low + high + 1) // 2
            if self.block_time(middle) <= cutoff:
                low = middle
            else:
                high = middle - 1
        return low

    def tx_search(self, query: str, per_page: int = 100) -> list[dict[str, Any]]:
        page = 1
        results: list[dict[str, Any]] = []
        while True:
            result = self.rpc(
                "tx_search",
                {"query": query, "prove": False, "page": str(page), "per_page": str(per_page), "order_by": "asc"},
            )
            txs = result.get("txs") or []
            results.extend(txs)
            total = int(result.get("total_count") or 0)
            if not txs or len(results) >= total:
                return results
            page += 1

    def decoded_tx(self, tx_hash: str) -> dict[str, Any]:
        return self.api(f"/cosmos/tx/v1beta1/txs/{tx_hash}")


def event_attributes(tx_response: dict[str, Any], event_type: str) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    for event in tx_response.get("events") or []:
        if event.get("type") == event_type:
            found.append({str(item["key"]): str(item["value"]) for item in event.get("attributes") or []})
    return found


def loya_received_by(tx_response: dict[str, Any], recipient: str) -> str | None:
    total = 0
    matched = False
    for attributes in event_attributes(tx_response, "transfer"):
        if attributes.get("recipient") != recipient:
            continue
        for coin in attributes.get("amount", "").split(","):
            if coin.endswith("loya") and coin[:-4].isdigit():
                total += int(coin[:-4]); matched = True
    return str(total) if matched else None
