# etl/sqlite_utils.py
import sqlite3, re
from typing import List, Dict, Any

def _safe_colname(name: str) -> str:
    n = re.sub(r'[^A-Za-z0-9_]', '_', name).lower()
    if n and n[0].isdigit():
        n = "c_" + n
    return n or "col"

def apply_sql_ddl_and_insert(ddl: str, rows: List[Dict[str, Any]], db_path: str = ":memory:", table_name: str = None):
    """
    Runs DDL in sqlite, inserts rows (rows: list of dicts), and returns rowcount.
    db_path: ':memory:' or filesystem path.
    If table_name is provided we try to use it; otherwise parse from DDL or use 'data'.
    Returns dict: { "db_path": db_path, "table": table_name, "inserted": n, "error": None }
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        # execute DDL
        cur.executescript(ddl)
        conn.commit()
    except Exception as e:
        return {"db_path": db_path, "table": table_name or "unknown", "inserted": 0, "error": f"DDL failed: {e}"}

    # try to infer table name if not provided
    if not table_name:
        m = re.search(r'create\s+table\s+([`"]?)(\w+)\1', ddl, re.I)
        if m:
            table_name = m.group(2)
        else:
            table_name = "data"

    # prepare insert
    if not rows:
        return {"db_path": db_path, "table": table_name, "inserted": 0, "error": None}

    # build column list from first row
    cols = []
    for k in rows[0].keys():
        cols.append(_safe_colname(k))
    placeholders = ", ".join(["?"] * len(cols))
    col_clause = ", ".join([f'"{c}"' for c in cols])
    insert_sql = f'INSERT INTO "{table_name}" ({col_clause}) VALUES ({placeholders})'
    inserted = 0
    for r in rows:
        vals = []
        for orig_k in list(rows[0].keys()):
            v = r.get(orig_k)
            vals.append(v)
        try:
            cur.execute(insert_sql, vals)
            inserted += 1
        except Exception:
            # try best-effort: coerce values to strings
            try:
                vals2 = [None if v is None else str(v) for v in vals]
                cur.execute(insert_sql, vals2)
                inserted += 1
            except Exception:
                # skip row
                continue
    conn.commit()
    conn.close()
    return {"db_path": db_path, "table": table_name, "inserted": inserted, "error": None}
