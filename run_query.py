#!/usr/bin/env python3
"""Run a SQL query against Logfood and print results as a table."""
import json, subprocess, sys

def run_query(sql_file, warehouse_id="927ac096f9833442"):
    with open(sql_file) as f:
        sql = f.read().strip()

    payload = json.dumps({
        "statement": sql,
        "warehouse_id": warehouse_id,
        "format": "JSON_ARRAY",
        "wait_timeout": "50s"
    })

    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements/",
         "--profile=logfood", f"--json={payload}"],
        capture_output=True, text=True
    )

    if not result.stdout.strip():
        print(f"ERROR: No output from databricks CLI. stderr: {result.stderr[:500]}")
        return

    d = json.loads(result.stdout)
    state = d.get("status", {}).get("state", "UNKNOWN")

    if state == "FAILED":
        err = d.get("status", {}).get("error", {})
        print(f"QUERY FAILED: {err.get('message', 'Unknown error')}")
        return

    if state == "PENDING" or state == "RUNNING":
        # Poll for result
        stmt_id = d.get("statement_id")
        print(f"Query still {state}, statement_id={stmt_id}")
        # Try to fetch
        if stmt_id:
            result2 = subprocess.run(
                ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}",
                 "--profile=logfood"],
                capture_output=True, text=True
            )
            d = json.loads(result2.stdout)
            state = d.get("status", {}).get("state")
            if state != "SUCCEEDED":
                print(f"Still {state}. Try again later.")
                return

    if state == "SUCCEEDED":
        manifest = d.get("manifest", {})
        cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]

        result_data = d.get("result", {})
        rows = result_data.get("data_array", [])

        if not rows:
            print("Query succeeded but returned 0 rows.")
            return

        # Calculate column widths
        widths = [len(c) for c in cols]
        for row in rows:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))

        # Print header
        header = "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
        print(header)
        print("-" * len(header))
        for row in rows:
            line = "  ".join(str(v if v is not None else "NULL").ljust(widths[i]) for i, v in enumerate(row))
            print(line)

        print(f"\n({len(rows)} rows)")
    else:
        print(f"Unexpected state: {state}")
        print(json.dumps(d, indent=2)[:1000])

if __name__ == "__main__":
    run_query(sys.argv[1])
