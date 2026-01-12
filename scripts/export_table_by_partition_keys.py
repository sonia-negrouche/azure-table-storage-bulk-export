#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bulk export Azure Table Storage entities to CSV using Azure AD auth.

What this does
- Reads PartitionKey values from a CSV/text file
- Queries Azure Table Storage REST API (AAD auth, no account keys required)
- Handles continuation tokens (pagination) + transient retries
- Runs requests concurrently (thread pool)
- Writes a single CSV where columns = union of keys found in returned entities

Auth
- Uses Interactive Browser auth by default; falls back to Device Code if needed.

Required (env or args)
- AZ_TABLE_URL            e.g. https://<account>.table.core.windows.net/<tableName>
- AZ_TENANT_ID            (optional) AAD tenant id; if omitted uses "common"

Input/Output
- --input   path to a file containing PartitionKey values (CSV w/ optional header "PartitionKey")
- --output  output CSV path

Examples:
  python scripts/export_table_by_partition_keys.py --input partition_keys.csv --output out.csv
  python scripts/export_table_by_partition_keys.py --input keys.csv --output out.csv --max-workers 12 --timeout 60
"""
from __future__ import annotations

import argparse
import csv
import time
import email.utils
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential, ChainedTokenCredential


DEFAULT_API_VERSION = "2023-01-03"
SCOPE = "https://storage.azure.com/.default"


def env(name: str) -> str:
    import os
    return (os.getenv(name) or "").strip()


def require(v: str, what: str, env_name: Optional[str] = None) -> str:
    v = (v or "").strip()
    if not v:
        hint = f" (env {env_name})" if env_name else ""
        raise SystemExit(f"Missing required {what}{hint}. Provide CLI arg or set env var.")
    return v


def load_partition_keys(path: str) -> List[str]:
    """
    Reads a file containing PartitionKey values.
    Accepted formats:
      - CSV with a single column
      - CSV with a header "PartitionKey"
      - Plain text (one value per line)
    Lines starting with '#' are ignored.
    """
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Input file not found: {p}")

    # Try CSV first; if it fails, fallback to line-by-line
    keys: List[str] = []
    with p.open(newline="", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)

        # Heuristic: if it looks like CSV (comma/semicolon or newlines with commas)
        looks_csv = ("," in sample) or (";" in sample) or ("PartitionKey" in sample)

        if looks_csv:
            rdr = csv.reader(f)
            rows = list(rdr)
            if not rows:
                return []
            # header detection
            first_cell = rows[0][0].strip() if rows[0] else ""
            start = 1 if first_cell.lower() == "partitionkey" else 0
            for row in rows[start:]:
                if not row:
                    continue
                pk = (row[0] or "").strip()
                if pk and not pk.startswith("#"):
                    keys.append(pk)
        else:
            for line in f:
                pk = (line or "").strip()
                if pk and not pk.startswith("#"):
                    keys.append(pk)

    # de-dupe, preserve order
    seen = set()
    out: List[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def get_credential(tenant_id: Optional[str]):
    """
    Interactive Browser first, then Device Code.
    This keeps the script usable on desktops and on headless environments.
    """
    tenant = (tenant_id or "").strip() or "common"
    ib = InteractiveBrowserCredential(tenant_id=tenant)
    dc = DeviceCodeCredential(tenant_id=tenant)
    return ChainedTokenCredential(ib, dc)


def auth_headers(token: str, api_version: str) -> Dict[str, str]:
    """
    Headers for Azure Tables REST API with AAD token.
    """
    # RFC1123 timestamp
    x_ms_date = email.utils.formatdate(usegmt=True)
    return {
        "Authorization": f"Bearer {token}",
        "x-ms-version": api_version,
        "x-ms-date": x_ms_date,
        "Accept": "application/json;odata=nometadata",
    }


def request_one_pk(
    pk: str,
    table_url: str,
    headers: Dict[str, str],
    timeout_sec: int,
    max_retries: int,
    initial_backoff: float,
) -> List[Dict[str, Any]]:
    """
    Fetch all entities for a given PartitionKey.
    Handles continuation tokens (NextPartitionKey/NextRowKey) and transient retries.
    """
    safe_pk = pk.replace("'", "''")  # OData escape
    params = {"$filter": f"PartitionKey eq '{safe_pk}'"}
    url = f"{table_url}()"

    results: List[Dict[str, Any]] = []
    next_params = dict(params)
    backoff = initial_backoff
    tries = 0

    while True:
        try:
            resp = requests.get(url, headers=headers, params=next_params, timeout=timeout_sec)

            if resp.status_code in (401, 403):
                raise RuntimeError(
                    f"{resp.status_code} for PartitionKey={pk}. "
                    "Check tenant and ensure RBAC role 'Storage Table Data Reader' (or equivalent) is granted."
                )

            # Throttling / transient errors
            if resp.status_code in (429, 500, 502, 503, 504):
                tries += 1
                if tries > max_retries:
                    resp.raise_for_status()
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []) or [])

            # Continuation headers
            npk = resp.headers.get("x-ms-continuation-NextPartitionKey")
            nrk = resp.headers.get("x-ms-continuation-NextRowKey")
            if npk or nrk:
                next_params = dict(params)
                if npk:
                    next_params["NextPartitionKey"] = npk
                if nrk:
                    next_params["NextRowKey"] = nrk
                continue

            break

        except requests.RequestException as e:
            tries += 1
            if tries > max_retries:
                raise RuntimeError(f"Request failed for PartitionKey={pk}: {e}") from e
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    return results


def write_csv(rows: List[Dict[str, Any]], out_path: str):
    """
    Writes all rows to CSV.
    Columns = union of all keys found across entities.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Determine columns
    colset = set()
    for r in rows:
        colset.update(r.keys())

    # Put common columns first if present
    preferred = ["PartitionKey", "RowKey", "Timestamp"]
    columns = [c for c in preferred if c in colset] + sorted([c for c in colset if c not in preferred])

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def parse_args():
    ap = argparse.ArgumentParser(description="Bulk export Azure Table Storage entities to CSV using Azure AD auth.")
    ap.add_argument("--table-url", default=env("AZ_TABLE_URL"), help="https://<account>.table.core.windows.net/<table>")
    ap.add_argument("--tenant-id", default=env("AZ_TENANT_ID"), help="AAD tenant id (optional, default: common)")
    ap.add_argument("--api-version", default=env("AZ_TABLE_API_VERSION") or DEFAULT_API_VERSION)
    ap.add_argument("--input", required=True, help="Input file containing PartitionKey values")
    ap.add_argument("--output", required=True, help="Output CSV path")

    ap.add_argument("--max-workers", type=int, default=int(env("AZ_MAX_WORKERS") or 10))
    ap.add_argument("--timeout", type=int, default=int(env("AZ_TIMEOUT_SEC") or 60))
    ap.add_argument("--max-retries", type=int, default=int(env("AZ_MAX_RETRIES") or 5))
    ap.add_argument("--initial-backoff", type=float, default=float(env("AZ_INITIAL_BACKOFF") or 1.0))
    return ap.parse_args()


def main():
    args = parse_args()
    table_url = require(args.table_url, "table url", "AZ_TABLE_URL").rstrip("/")
    tenant_id = (args.tenant_id or "").strip() or None
    api_version = (args.api_version or DEFAULT_API_VERSION).strip()

    keys = load_partition_keys(args.input)
    if not keys:
        raise SystemExit("No PartitionKey values found in input file.")

    cred = get_credential(tenant_id)
    token = cred.get_token(SCOPE).token
    headers = auth_headers(token, api_version)

    print(f"PartitionKeys: {len(keys)} | max_workers={args.max_workers} | timeout={args.timeout}s")

    all_rows: List[Dict[str, Any]] = []
    failures: List[str] = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {
            ex.submit(
                request_one_pk,
                pk,
                table_url,
                headers,
                args.timeout,
                args.max_retries,
                args.initial_backoff,
            ): pk
            for pk in keys
        }

        for fut in as_completed(futs):
            pk = futs[fut]
            try:
                rows = fut.result()
                all_rows.extend(rows)
                print(f"OK  {pk} -> {len(rows)} rows")
            except Exception as e:
                failures.append(pk)
                print(f"ERR {pk}: {e}")

    write_csv(all_rows, args.output)

    print("\n----- SUMMARY -----")
    print(f"PartitionKeys processed: {len(keys)}")
    print(f"Total rows exported:     {len(all_rows)}")
    print(f"Failures:               {len(failures)}")
    if failures:
        print("Failed PartitionKeys (first 20):", failures[:20])
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
