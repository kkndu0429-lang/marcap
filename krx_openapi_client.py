from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable


BASE_URL = "https://data-dbg.krx.co.kr/svc/apis"
DATE_RE = re.compile(r"^\d{8}$")


@dataclass(frozen=True)
class Service:
    name: str
    endpoint: str
    kind: str
    required_fields: tuple[str, ...]
    key_fields: tuple[str, ...]


SERVICES: dict[str, Service] = {
    "kospi_index": Service(
        "KOSPI 시리즈 일별시세정보",
        "idx/kospi_dd_trd",
        "index",
        ("BAS_DD", "IDX_NM", "CLSPRC_IDX"),
        ("BAS_DD", "IDX_NM"),
    ),
    "kosdaq_index": Service(
        "KOSDAQ 시리즈 일별시세정보",
        "idx/kosdaq_dd_trd",
        "index",
        ("BAS_DD", "IDX_NM", "CLSPRC_IDX"),
        ("BAS_DD", "IDX_NM"),
    ),
    "kospi_stock": Service(
        "유가증권 일별매매정보",
        "sto/stk_bydd_trd",
        "stock",
        ("BAS_DD", "ISU_CD", "TDD_CLSPRC", "ACC_TRDVOL", "ACC_TRDVAL"),
        ("BAS_DD", "ISU_CD"),
    ),
    "kosdaq_stock": Service(
        "코스닥 일별매매정보",
        "sto/ksq_bydd_trd",
        "stock",
        ("BAS_DD", "ISU_CD", "TDD_CLSPRC", "ACC_TRDVOL", "ACC_TRDVAL"),
        ("BAS_DD", "ISU_CD"),
    ),
}


class KrxOpenApiError(RuntimeError):
    pass


def _compact_number(value: Any) -> float:
    if value is None or value == "":
        raise ValueError("empty numeric value")
    return float(str(value).replace(",", "").strip())


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def validate_rows(service: Service, requested_date: str, rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        raise KrxOpenApiError(f"{service.name}: OutBlock_1 is empty")
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise KrxOpenApiError(f"{service.name}: row {index} is not an object")
        missing = [field for field in service.required_fields if field not in row]
        if missing:
            raise KrxOpenApiError(f"{service.name}: row {index} missing fields {missing}")
        row_date = str(row["BAS_DD"]).replace("-", "")
        if row_date != requested_date:
            raise KrxOpenApiError(
                f"{service.name}: requested {requested_date}, received {row_date}"
            )
        key = tuple(str(row[field]) for field in service.key_fields)
        if key in seen:
            raise KrxOpenApiError(f"{service.name}: duplicate key {key}")
        seen.add(key)
        try:
            if service.kind == "stock":
                for field in ("TDD_CLSPRC", "ACC_TRDVOL", "ACC_TRDVAL"):
                    if _compact_number(row[field]) < 0:
                        raise ValueError(f"{field} is negative")
            else:
                close_value = row["CLSPRC_IDX"]
                if close_value not in (None, ""):
                    _compact_number(close_value)
                else:
                    # KRX includes "(외국주포함)" aggregate rows whose index
                    # price fields are blank but volume/value fields are valid.
                    _compact_number(row.get("ACC_TRDVOL"))
                    _compact_number(row.get("ACC_TRDVAL"))
        except ValueError as exc:
            raise KrxOpenApiError(f"{service.name}: row {index} invalid number: {exc}") from exc
        normalized.append(dict(row))
    if service.kind == "index":
        primary_name = "코스피" if "kospi" in service.endpoint else "코스닥"
        primary_rows = [row for row in normalized if str(row.get("IDX_NM", "")).strip() == primary_name]
        if len(primary_rows) != 1:
            raise KrxOpenApiError(f"{service.name}: primary index row {primary_name} missing")
        try:
            _compact_number(primary_rows[0].get("CLSPRC_IDX"))
        except ValueError as exc:
            raise KrxOpenApiError(f"{service.name}: primary index close is invalid") from exc
    return normalized


class KrxOpenApiClient:
    def __init__(
        self,
        auth_key: str,
        *,
        base_url: str = BASE_URL,
        auth_mode: str = "query",
        timeout: int = 30,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        if not auth_key or not auth_key.strip():
            raise KrxOpenApiError("KRX_AUTH_KEY is missing")
        if auth_mode not in {"query", "header"}:
            raise ValueError("auth_mode must be query or header")
        self._auth_key = auth_key.strip()
        self._base_url = base_url.rstrip("/")
        self._auth_mode = auth_mode
        self._timeout = timeout
        self._opener = opener

    def fetch(self, service_id: str, bas_dd: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if service_id not in SERVICES:
            raise KrxOpenApiError(f"unknown service: {service_id}")
        if not DATE_RE.fullmatch(bas_dd):
            raise KrxOpenApiError("bas_dd must be YYYYMMDD")
        service = SERVICES[service_id]
        params = {"basDd": bas_dd}
        headers = {"Accept": "application/json", "User-Agent": "stock-report-krx-client/1"}
        if self._auth_mode == "query":
            params["AUTH_KEY"] = self._auth_key
        else:
            headers["AUTH_KEY"] = self._auth_key
        url = f"{self._base_url}/{service.endpoint}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            response = self._opener(request, timeout=self._timeout)
            with response:
                body = response.read()
                http_status = int(getattr(response, "status", 200))
        except urllib.error.HTTPError as exc:
            raise KrxOpenApiError(f"{service.name}: HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise KrxOpenApiError(f"{service.name}: network error {exc.reason}") from exc
        if http_status != 200:
            raise KrxOpenApiError(f"{service.name}: HTTP {http_status}")
        try:
            payload = json.loads(body.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KrxOpenApiError(f"{service.name}: invalid JSON") from exc
        if not isinstance(payload, dict) or "OutBlock_1" not in payload:
            raise KrxOpenApiError(f"{service.name}: missing OutBlock_1")
        rows = validate_rows(service, bas_dd, payload["OutBlock_1"])
        meta = {
            "service_id": service_id,
            "service_name": service.name,
            "endpoint": service.endpoint,
            "requested_date": bas_dd,
            "source_date": bas_dd,
            "http_status": http_status,
            "row_count": len(rows),
            "response_sha256": hashlib.sha256(body).hexdigest(),
            "auth_mode": self._auth_mode,
        }
        return rows, meta


def collect(
    client: KrxOpenApiClient,
    bas_dd: str,
    service_ids: Iterable[str],
    output_root: Path,
) -> list[dict[str, Any]]:
    run_at = datetime.now().astimezone().isoformat(timespec="seconds")
    statuses: list[dict[str, Any]] = []
    for service_id in service_ids:
        try:
            rows, meta = client.fetch(service_id, bas_dd)
            payload = {
                "schema_version": 1,
                "source": "KRX_OPEN_API",
                "collected_at": run_at,
                **meta,
                "rows": rows,
            }
            target = output_root / "snapshots" / bas_dd / f"{service_id}.json"
            _atomic_write(
                target,
                json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            status = {"collected_at": run_at, "status": "valid", **meta}
        except Exception as exc:
            status = {
                "collected_at": run_at,
                "status": "source_error",
                "service_id": service_id,
                "requested_date": bas_dd,
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
        statuses.append(status)
        _append_jsonl(output_root / "logs" / "krx_openapi_runs.jsonl", status)
    return statuses


def collect_latest(
    client: KrxOpenApiClient,
    latest_date: str,
    service_ids: Iterable[str],
    output_root: Path,
    *,
    lookback_days: int = 10,
) -> tuple[str | None, list[dict[str, Any]]]:
    service_ids = list(service_ids)
    latest = datetime.strptime(latest_date, "%Y%m%d").date()
    attempts: list[dict[str, Any]] = []
    for offset in range(lookback_days + 1):
        candidate = (latest - timedelta(days=offset)).strftime("%Y%m%d")
        statuses = collect(client, candidate, service_ids, output_root)
        if all(row["status"] == "valid" for row in statuses):
            selection = {
                "collected_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "status": "valid",
                "requested_latest_date": latest_date,
                "selected_source_date": candidate,
                "service_count": len(service_ids),
                "attempted_dates": [*attempts, {"date": candidate, "status": "valid"}],
            }
            _atomic_write(
                output_root / "latest_status.json",
                json.dumps(selection, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            _append_jsonl(output_root / "logs" / "krx_openapi_selections.jsonl", selection)
            return candidate, statuses
        attempts.append({
            "date": candidate,
            "status": "source_error",
            "failed_services": [
                row["service_id"] for row in statuses if row["status"] != "valid"
            ],
        })
    selection = {
        "collected_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": "source_error",
        "requested_latest_date": latest_date,
        "selected_source_date": None,
        "service_count": len(service_ids),
        "attempted_dates": attempts,
    }
    _atomic_write(
        output_root / "latest_status.json",
        json.dumps(selection, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    _append_jsonl(output_root / "logs" / "krx_openapi_selections.jsonl", selection)
    return None, []


def main() -> int:
    parser = argparse.ArgumentParser(description="Isolated KRX Open API collector")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument(
        "--services",
        nargs="+",
        choices=sorted(SERVICES),
        default=sorted(SERVICES),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Search backward for the newest common valid service date",
    )
    parser.add_argument("--lookback-days", type=int, default=10)
    parser.add_argument(
        "--auth-mode",
        choices=("query", "header"),
        default=os.getenv("KRX_OPENAPI_AUTH_MODE", "query"),
    )
    args = parser.parse_args()
    try:
        client = KrxOpenApiClient(
            os.getenv("KRX_AUTH_KEY", ""),
            auth_mode=args.auth_mode,
        )
        if args.latest:
            selected_date, statuses = collect_latest(
                client,
                args.date,
                args.services,
                args.output,
                lookback_days=max(args.lookback_days, 0),
            )
            if selected_date is None:
                print(json.dumps({
                    "status": "source_error",
                    "requested_latest_date": args.date,
                    "message": "no common valid KRX Open API date",
                }, ensure_ascii=False))
                return 1
        else:
            statuses = collect(client, args.date, args.services, args.output)
    except Exception as exc:
        print(json.dumps({"status": "failed", "message": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(statuses, ensure_ascii=False, indent=2))
    return 0 if all(row["status"] == "valid" for row in statuses) else 1


if __name__ == "__main__":
    raise SystemExit(main())
