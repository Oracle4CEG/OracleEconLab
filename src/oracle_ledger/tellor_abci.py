"""Direct historical ABCI queries for Tellor Layer oracle reports.

The public REST gateway is only a JSON translation of this protobuf gRPC
query.  Calling the same query through the archive CometBFT RPC preserves the
canonical application-state evidence while avoiding an additional gateway
rate limit.
"""
from __future__ import annotations

import base64
import itertools
from typing import Any

import requests


QUERY_PATH = "/layer.oracle.Query/GetReportsbyReporter"
REQUEST_IDS = itertools.count(1)


def _varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("protobuf varints must be non-negative")
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _bytes_field(number: int, value: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(value)) + value


def _varint_field(number: int, value: int) -> bytes:
    return _varint(number << 3) + _varint(value)


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("truncated or oversized protobuf varint")


def _fields(data: bytes) -> list[tuple[int, int, int | bytes]]:
    output: list[tuple[int, int, int | bytes]] = []
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise ValueError("invalid protobuf field number zero")
        if wire_type == 0:
            value, offset = _read_varint(data, offset)
        elif wire_type == 1:
            if offset + 8 > len(data):
                raise ValueError("truncated protobuf fixed64")
            value = data[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(data, offset)
            if offset + length > len(data):
                raise ValueError("truncated protobuf bytes field")
            value = data[offset : offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(data):
                raise ValueError("truncated protobuf fixed32")
            value = data[offset : offset + 4]
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
        output.append((number, wire_type, value))
    return output


def _single(fields: list[tuple[int, int, int | bytes]], number: int) -> int | bytes | None:
    values = [value for field, _wire, value in fields if field == number]
    if len(values) > 1:
        raise ValueError(f"duplicate singular protobuf field {number}")
    return values[0] if values else None


def _text(value: int | bytes | None, name: str) -> str:
    if not isinstance(value, bytes):
        raise ValueError(f"missing or invalid protobuf string {name}")
    return value.decode("utf-8")


def _integer(value: int | bytes | None, name: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"missing or invalid protobuf integer {name}")
    return value


def encode_reports_by_reporter_request(
    reporter: str,
    params: dict[str, str],
) -> bytes:
    pagination = bytearray()
    if params.get("pagination.key"):
        pagination += _bytes_field(
            1,
            base64.b64decode(params["pagination.key"], validate=True),
        )
    if params.get("pagination.offset"):
        pagination += _varint_field(2, int(params["pagination.offset"]))
    if params.get("pagination.limit"):
        pagination += _varint_field(3, int(params["pagination.limit"]))
    if params.get("pagination.count_total", "").lower() == "true":
        pagination += _varint_field(4, 1)
    if params.get("pagination.reverse", "").lower() == "true":
        pagination += _varint_field(5, 1)
    return _bytes_field(1, reporter.encode("utf-8")) + _bytes_field(2, bytes(pagination))


def decode_reports_by_reporter_response(data: bytes) -> dict[str, Any]:
    response_fields = _fields(data)
    reports: list[dict[str, Any]] = []
    pagination: dict[str, Any] = {"next_key": None, "total": "0"}
    for number, wire_type, value in response_fields:
        if number == 1:
            if wire_type != 2 or not isinstance(value, bytes):
                raise ValueError("invalid MicroReportStrings response field")
            fields = _fields(value)
            query_id = _text(_single(fields, 4), "query_id").lower().removeprefix("0x")
            if len(query_id) != 64:
                raise ValueError(f"invalid Tellor query id: {query_id}")
            reports.append(
                {
                    "reporter": _text(_single(fields, 1), "reporter"),
                    "power": str(_integer(_single(fields, 2), "power")),
                    "query_type": _text(_single(fields, 3), "query_type"),
                    "query_id": query_id,
                    "aggregate_method": _text(
                        _single(fields, 5),
                        "aggregate_method",
                    ),
                    "value": _text(_single(fields, 6), "value"),
                    "timestamp": str(_integer(_single(fields, 7), "timestamp")),
                    "cyclelist": bool(_integer(_single(fields, 8), "cyclelist"))
                    if _single(fields, 8) is not None
                    else False,
                    "block_number": str(
                        _integer(_single(fields, 9), "block_number")
                    ),
                    "meta_id": str(_integer(_single(fields, 10), "meta_id")),
                }
            )
        elif number == 2:
            if wire_type != 2 or not isinstance(value, bytes):
                raise ValueError("invalid PageResponse field")
            fields = _fields(value)
            next_key = _single(fields, 1)
            total = _single(fields, 2)
            pagination = {
                "next_key": (
                    base64.b64encode(next_key).decode("ascii")
                    if isinstance(next_key, bytes) and next_key
                    else None
                ),
                "total": str(total if isinstance(total, int) else 0),
            }
    return {"microReports": reports, "pagination": pagination}


def reports_by_reporter_rpc_request(
    reporter: str,
    height: int,
    params: dict[str, str],
) -> tuple[int, dict[str, Any]]:
    request = encode_reports_by_reporter_request(reporter, params)
    identifier = next(REQUEST_IDS)
    return identifier, {
        "jsonrpc": "2.0",
        "id": identifier,
        "method": "abci_query",
        "params": {
            "path": QUERY_PATH,
            "data": request.hex(),
            "height": str(height),
            "prove": False,
        },
    }


def decode_reports_by_reporter_rpc(
    body: dict[str, Any],
    identifier: int,
    height: int,
) -> dict[str, Any]:
    if body.get("error"):
        raise RuntimeError(str(body["error"]))
    if int(body.get("id")) != identifier:
        raise RuntimeError("Tellor ABCI response id mismatch")
    result = body.get("result") or {}
    abci = result.get("response") or {}
    if int(abci.get("code") or 0) != 0:
        raise RuntimeError(
            f"Tellor ABCI query failed with code {abci.get('code')}: {abci.get('log')}"
        )
    response_height = int(abci.get("height") or 0)
    if response_height != height:
        raise RuntimeError(
            f"Tellor ABCI historical height mismatch: {response_height} != {height}"
        )
    encoded = abci.get("value")
    if not isinstance(encoded, str):
        raise RuntimeError("Tellor ABCI query omitted protobuf response")
    return decode_reports_by_reporter_response(
        base64.b64decode(encoded, validate=True)
    )


def reports_by_reporter_abci(
    session: requests.Session,
    rpc_url: str,
    reporter: str,
    height: int,
    params: dict[str, str],
    *,
    timeout: int,
) -> dict[str, Any]:
    identifier, payload = reports_by_reporter_rpc_request(
        reporter,
        height,
        params,
    )
    response = session.post(
        rpc_url.rstrip("/"),
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    return decode_reports_by_reporter_rpc(
        response.json(),
        identifier,
        height,
    )
