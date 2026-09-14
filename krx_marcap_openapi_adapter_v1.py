from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd


OUTPUT_COLUMNS = [
    "Code", "Name", "Close", "Dept", "ChangeCode", "Changes", "ChangesRatio",
    "Volume", "Amount", "Open", "High", "Low", "Marcap", "Stocks",
    "Market", "MarketId", "Date", "Rank",
]


def _number(value):
    if value in (None, ""):
        return pd.NA
    return pd.to_numeric(str(value).replace(",", "").strip(), errors="coerce")


def normalize_rows(rows: list[dict], bas_dd: str, market_id: str) -> pd.DataFrame:
    records = []
    for row in rows:
        records.append({
            "Code": str(row.get("ISU_CD", "")).strip(),
            "Name": row.get("ISU_NM", ""),
            "Close": _number(row.get("TDD_CLSPRC")),
            "Dept": row.get("SECT_TP_NM", ""),
            "ChangeCode": row.get("FLUC_TP_CD", ""),
            "Changes": _number(row.get("CMPPREVDD_PRC")),
            "ChangesRatio": _number(row.get("FLUC_RT")),
            "Volume": _number(row.get("ACC_TRDVOL")),
            "Amount": _number(row.get("ACC_TRDVAL")),
            "Open": _number(row.get("TDD_OPNPRC")),
            "High": _number(row.get("TDD_HGPRC")),
            "Low": _number(row.get("TDD_LWPRC")),
            "Marcap": _number(row.get("MKTCAP")),
            "Stocks": _number(row.get("LIST_SHRS")),
            "Market": row.get("MKT_NM", ""),
            "MarketId": market_id,
            "Date": pd.to_datetime(bas_dd, format="%Y%m%d"),
        })
    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError(f"empty KRX Open API rows: {market_id} {bas_dd}")
    if frame["Code"].eq("").any() or frame["Code"].duplicated().any():
        raise ValueError(f"invalid or duplicate Code: {market_id} {bas_dd}")
    frame = frame.sort_values(["Marcap", "Code"], ascending=[False, True], na_position="last")
    frame["Rank"] = range(1, len(frame) + 1)
    return frame[OUTPUT_COLUMNS]


def load_snapshot(root: Path, bas_dd: str, service: str) -> tuple[list[dict], dict]:
    path = root / "snapshots" / bas_dd / f"{service}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("source") != "KRX_OPEN_API" or payload.get("source_date") != bas_dd:
        raise ValueError(f"snapshot metadata mismatch: {path}")
    return payload["rows"], payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only KRX Open API MARCAP adapter")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--snapshot-root", type=Path, help="Use existing KRX Open API snapshots; no network")
    args = parser.parse_args()

    if args.snapshot_root:
        kospi_rows, kospi_meta = load_snapshot(args.snapshot_root, args.date, "kospi_stock")
        kosdaq_rows, kosdaq_meta = load_snapshot(args.snapshot_root, args.date, "kosdaq_stock")
        konex_rows, konex_meta = load_snapshot(args.snapshot_root, args.date, "konex_stock")
    else:
        import krx_openapi_client
        from krx_openapi_client import KrxOpenApiClient, Service

        # KONEX is intentionally added only in this research adapter. The
        # operating client remains unchanged until a separate approval.
        krx_openapi_client.SERVICES["konex_stock"] = Service(
            "코넥스 일별매매정보",
            "sto/knx_bydd_trd",
            "stock",
            ("BAS_DD", "ISU_CD", "TDD_CLSPRC", "ACC_TRDVOL", "ACC_TRDVAL"),
            ("BAS_DD", "ISU_CD"),
        )
        client = KrxOpenApiClient(os.getenv("KRX_AUTH_KEY", ""))
        kospi_rows, kospi_meta = client.fetch("kospi_stock", args.date)
        kosdaq_rows, kosdaq_meta = client.fetch("kosdaq_stock", args.date)
        konex_rows, konex_meta = client.fetch("konex_stock", args.date)

    frame = pd.concat([
        normalize_rows(kospi_rows, args.date, "STK"),
        normalize_rows(kosdaq_rows, args.date, "KSQ"),
        normalize_rows(konex_rows, args.date, "KNX"),
    ], ignore_index=True)
    if frame["Code"].duplicated().any():
        raise ValueError(f"cross-market duplicate Code: {frame.loc[frame['Code'].duplicated(), 'Code'].tolist()}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(args.output, index=False, compression="snappy")
    result = {
        "status": "PASS",
        "source": "KRX_OPEN_API",
        "source_date": args.date,
        "rows": len(frame),
        "unique_codes": int(frame["Code"].nunique()),
        "date_min": frame["Date"].min().strftime("%Y-%m-%d"),
        "date_max": frame["Date"].max().strftime("%Y-%m-%d"),
        "markets": {
            "STK": len(kospi_rows),
            "KSQ": len(kosdaq_rows),
            "KNX": len(konex_rows),
        },
        "output": str(args.output),
        "metadata": {
            "kospi_response_sha256": kospi_meta.get("response_sha256"),
            "kosdaq_response_sha256": kosdaq_meta.get("response_sha256"),
            "konex_response_sha256": konex_meta.get("response_sha256"),
        },
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
