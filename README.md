# Azure Table Storage Bulk Export (Python)

Bulk export **Azure Table Storage** entities to **CSV** using **Azure AD authentication**.

This repository provides a lightweight, production-friendly script to export large volumes of entities
by **PartitionKey**, with:
- Azure AD auth (no account keys required)
- pagination (continuation tokens)
- transient retries (429/5xx)
- concurrency (thread pool)
- CSV output with automatic column union

---

## Why this repository exists

Exporting large datasets from Azure Table Storage often becomes painful when:
- you must avoid using account keys (security / enterprise constraints)
- results are too large for manual/portal workflows
- you need stable pagination, retries, and concurrency

This script is designed for **cloud ops / DevOps / platform** scenarios and investigations.

---

## Requirements

- Python 3.10+ recommended
- Packages:
  - `azure-identity`
  - `requests`

Install:

```bash
pip install -r requirements.txt
```

---

## Authentication & permissions

The script uses Azure AD to request a token for `https://storage.azure.com/.default`.

Your identity must have an appropriate RBAC role on the storage account, such as:
- **Storage Table Data Reader** (read)
- **Storage Table Data Contributor** (read/write)

> Exact role requirements depend on your environment.

---

## Configuration

You can provide values via environment variables or CLI arguments.

### Environment variables

- `AZ_TABLE_URL` (required)  
  Example: `https://<account>.table.core.windows.net/<tableName>`

Optional:
- `AZ_TENANT_ID` (optional; default: `common`)
- `AZ_TABLE_API_VERSION` (default: `2023-01-03`)
- `AZ_MAX_WORKERS` (default: `10`)
- `AZ_TIMEOUT_SEC` (default: `60`)
- `AZ_MAX_RETRIES` (default: `5`)
- `AZ_INITIAL_BACKOFF` (default: `1.0`)

---

## Usage

### 1) Prepare an input file

The input file can be:
- a CSV with a single column, or a header `PartitionKey`
- or a plain text file with one PartitionKey per line

Example `partition_keys.csv`:

```csv
PartitionKey
PK_001
PK_002
PK_003
```

---

### 2) Configure environment

Bash:

```bash
export AZ_TABLE_URL="https://<account>.table.core.windows.net/<tableName>"
export AZ_TENANT_ID="<your-tenant-id>"   # optional
```

PowerShell:

```powershell
$env:AZ_TABLE_URL="https://<account>.table.core.windows.net/<tableName>"
$env:AZ_TENANT_ID="<your-tenant-id>"   # optional
```

---

### 3) Run

```bash
python scripts/export_table_by_partition_keys.py \
  --input partition_keys.csv \
  --output out.csv \
  --max-workers 10 \
  --timeout 60
```

---

## Output format

The script writes a CSV with columns equal to the **union of all keys** present across exported entities.
Common columns (`PartitionKey`, `RowKey`, `Timestamp`) are placed first when present.

---

## Notes

- Start with conservative settings (`--max-workers 4` or `6`) and increase gradually.
- For throttling (HTTP 429), reduce concurrency and/or increase backoff.
- For very large exports, consider splitting the input keys file and running in batches.

---

## Security

- No secrets are stored in code.
- Do not commit `.env` files.
- If any secret appears in Git history, revoke it immediately.

---

## License

MIT
