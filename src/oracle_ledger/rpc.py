"""Small JSON-RPC client with deterministic on-disk request/response evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class RpcError(RuntimeError):
    """A JSON-RPC transport or protocol error."""


@dataclass
class JsonRpc:
    url: str
    timeout_seconds: int = 120
    next_id: int = 1

    def call(self, method: str, params: list[Any]) -> Any:
        request_id = self.next_id
        self.next_id += 1
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        request = Request(
            self.url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            # Chainstack rejects otherwise valid JSON-RPC requests without a
            # user agent. Reth accepts it as well, so use one consistently.
            headers={"Content-Type": "application/json", "User-Agent": "oracle-honesty-ledger/0.1"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RpcError(f"{method} transport failure: {exc}") from exc
        if "error" in decoded:
            raise RpcError(f"{method} RPC error: {decoded['error']}")
        if "result" not in decoded:
            raise RpcError(f"{method} missing result: {decoded}")
        return decoded["result"]


def write_json(path: Path, value: Any) -> None:
    """Atomically write canonical JSON. Amounts remain hexadecimal or decimal strings."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)
