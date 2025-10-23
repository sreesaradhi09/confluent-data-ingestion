
#!/usr/bin/env python3
# Flink SQL Generator for STTM v20
import re
from pathlib import Path
from typing import Dict, List, Tuple, OrderedDict
import pandas as pd

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").map(lambda x: "" if x.strip().lower()=="nan" else x.strip())
    return df

def read_cfg(xl: pd.ExcelFile) -> pd.DataFrame:
    try:
        cfg = pd.read_excel(xl, sheet_name="Config", dtype=str).fillna("")
        cfg = norm_cols(cfg)
    except Exception:
        cfg = pd.DataFrame({"key":[], "value":[]})
    return cfg

def cfg_get(cfg: pd.DataFrame, key: str, default: str = "") -> str:
    if "key" not in cfg.columns or "value" not in cfg.columns:
        return default
    m = cfg[cfg["key"]==key]
    if m.empty: return default
    v = str(m["value"].iloc[0]).strip()
    return default if v.lower()=="nan" else v

def lint_json_key(key: str) -> str:
    if key == "":
        return "empty key"
    if key.startswith("$"):
        return "key must not start with '$'"
    if not re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*", key):
        return "invalid chars (use A-Za-z0-9_ with dots)"
    return ""

def stage_rank(s: str) -> int:
    u = (s or "").upper()
    return {"VIEW":0,"XREF":1,"FGAC":2}.get(u, 99)


def choose_expr(row: dict, is_view: bool) -> str:
    override = row.get("ExprOverride","").strip()
    stx = row.get("SourceTransformExpr","").strip()
    tgt = row.get("TargetDataType","STRING").strip() or "STRING"
    if is_view:
        if override:
            import re as _re
            return override if _re.match(r"(?is)^\s*CAST\s*\(", override) else f"CAST({override} AS {tgt})"
        if stx:
            import re as _re
            return stx if _re.match(r"(?is)^\s*CAST\s*\(", stx) else f"CAST({stx} AS {tgt})"
        mf   = (row.get("MessageFormat") or "").upper()
        src  = (row.get("SourceField") or "val").strip()
        key  = (row.get("FieldSelector") or "").strip()
        if mf == "JSON":
            base = f"JSON_VALUE(CAST({src} AS STRING), '$.{key}')"
        elif mf == "CSV":
            base = f"SPLIT_INDEX(CAST({src} AS STRING), ',', CAST({key} AS INT))"
        else:
            base = src
        norm = f"TRIM({base})" if tgt.upper().startswith("STRING") else f"NULLIF(TRIM({base}), '')"
        return f"CAST({norm} AS {tgt})"
    # Non-view (XREF/FGAC): no auto-cast or auto JSON/CSV; use as-is precedence
    if override:
        return override
    if stx:
        return stx
    sf = (row.get("SourceField") or "").strip()
    if sf:
        return sf
    return row.get("TargetColumn","").strip() or "NULL"
def build_view_sql(table: str, rows: List[dict]) -> str:
    selects = []
    for r in rows:
        expr = choose_expr(r, True)
        selects.append(f"  {expr} AS {r['TargetColumn']}")
    # FilterPredicate only on PK row
    pk_filter = ""
    for r in rows:
        if (r.get("IsTargetPK","").upper()=="Y") and r.get("FilterPredicate","").strip():
            pk_filter = r["FilterPredicate"].strip()
            break
    where = f"\nWHERE {pk_filter}" if pk_filter else ""
    src = next((f"{r.get('SourcePrimaryTable','')} {r.get('SourcePrimaryAlias','') or 't'}" for r in rows if r.get("SourcePrimaryTable","")), "")
    return f"CREATE VIEW {table} AS\nSELECT\n" + ",\n".join(selects) + f"\nFROM {src}{where};"

def iceberg_table_ddl(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    # respect order, dedupe
    seen = set(); col_lines = []
    pk = []
    for r in rows:
        c = r["TargetColumn"]; t = r["TargetDataType"]
        if c and t and c not in seen:
            col_lines.append(f"  {c} {t}")
            seen.add(c)
        if (r.get("IsTargetPK","").upper()=="Y") and c not in pk:
            pk.append(c)
    vp = cfg_get(cfg, "table_value_format", "avro-registry")
    props = [f"'value.format'='{vp}'"]
    if pk:
        col_lines.append("  " + f",PRIMARY KEY ({', '.join(pk)}) NOT ENFORCED")
    return "CREATE TABLE IF NOT EXISTS " + table + " (\n" + ",\n".join(col_lines) + "\n)\nWITH (\n  " + ", ".join(props) + "\n);"

def join_clause(rows: List[dict]) -> str:
    # join only from the first row that has JoinTable+JoinCondition; default LEFT if type missing
    for r in rows:
        jt = r.get("JoinTable","").strip()
        jc = r.get("JoinCondition","").strip()
        if jt and jc:
            jty = (r.get("JoinType","") or "LEFT").upper()
            if jty not in {"INNER","LEFT","RIGHT","FULL"}:
                jty = "LEFT"
            ja = r.get("JoinAlias","").strip() or "j"
            return f"\n  {jty} JOIN {jt} {ja} ON {jc}"
    return ""

def insert_sql(table: str, rows: List[dict]) -> str:
    cols = [r["TargetColumn"] for r in rows if r.get("TargetColumn")]
    selects = [f"  {choose_expr(r, False)} AS {r['TargetColumn']}" for r in rows if r.get("TargetColumn")]
    drv = next((f"{r.get('SourcePrimaryTable','')} {r.get('SourcePrimaryAlias','') or 't'}" for r in rows if r.get("SourcePrimaryTable","")), "")
    join = join_clause(rows)
    return f"INSERT INTO {table} ({', '.join(cols)})\nSELECT\n" + ",\n".join(selects) + f"\nFROM {drv}{join};"

def apply_prefix_suffix(name: str, cfg: pd.DataFrame, is_view: bool) -> str:
    if is_view:
        pref = cfg_get(cfg, "view_prefix", "")
        suff = cfg_get(cfg, "view_suffix", "")
    else:
        pref = cfg_get(cfg, "table_prefix", "")
        suff = cfg_get(cfg, "table_suffix", "")
    return f"{pref}{name}{suff}"

def classify_target(name: str) -> str:
    u = name.upper()
    if name.lower().endswith("_view"):
        return "VIEW"
    if u.startswith("XREF_"):
        return "XREF"
    return "FGAC"

def validate(sttm: pd.DataFrame) -> List[str]:
    issues = []
    # Views must have either ExprOverride or (MessageFormat + FieldSelector) for auto; SourceField is payload column
    views = sttm[sttm["PipelineStage"].str.upper()=="VIEW"]
    for i, r in views.iterrows():
        override = r.get("ExprOverride","").strip()
        mf = (r.get("MessageFormat","")).upper()
        key = r.get("FieldSelector","").strip()
        src = r.get("SourceField","").strip()
        if not src:
            issues.append(f"WARN row {i}: View SourceField (payload column) is empty")
        if not override and mf=="JSON" and not key:
            issues.append(f"WARN row {i}: JSON View missing FieldSelector (JSON key)")
        if mf=="JSON" and key:
            msg = lint_json_key(key)
            if msg and msg!="empty key":
                issues.append(f"WARN row {i}: FieldSelector {msg}: '{key}'")
    # Filter predicate only on PK row for each view table
    for tt, grp in views.groupby("TargetTable"):
        nonpk = grp[(grp["IsTargetPK"].str.upper()!="Y") & (grp["FilterPredicate"]!="")]
        if not nonpk.empty:
            issues.append(f"WARN table {tt}: FilterPredicate present on non-PK rows")
        pk_with = grp[(grp["IsTargetPK"].str.upper()=="Y") & (grp["FilterPredicate"]!="")]
        if len(pk_with.index) > 1:
            issues.append(f"WARN table {tt}: multiple FilterPredicate values on PK rows")
    # Joins: if condition present and type missing, default LEFT (we don't record as issue)
    jf = sttm[(sttm["JoinCondition"]!="")]
    for i, r in jf.iterrows():
        jt = (r.get("JoinType","") or "LEFT").upper()
        if jt not in {"INNER","LEFT","RIGHT","FULL"}:
            issues.append(f"WARN row {i}: invalid JoinType '{jt}' (defaulting to LEFT)")
    return issues

def generate(sttm_path: Path, out_dir: Path):
    xl = pd.ExcelFile(sttm_path)
    cfg = read_cfg(xl)
    df = norm_cols(pd.read_excel(xl, sheet_name="STTM_Mapping", dtype=str).fillna(""))
    # Sort for stability
    df["_s"] = df["PipelineStage"].apply(stage_rank)
    df["_p"] = df["IsTargetPK"].str.upper().map(lambda x: 0 if x=="Y" else 1)
    df = df.sort_values(by=["_s","TargetTable","_p","TargetColumn"]).drop(columns=["_s","_p"])

    issues = validate(df)

    grouped: Dict[str, List[dict]] = {}
    for _, r in df.iterrows():
        row = {c: str(r[c]).strip() if c in df.columns else "" for c in df.columns}
        t = row.get("TargetTable","")
        if not t: 
            continue
        grouped.setdefault(t, []).append(row)

    out_dir.mkdir(parents=True, exist_ok=True)
    views_sql, ddls_sql, xref_sql, fgac_sql = [], [], [], []
    for t, rows in grouped.items():
        kind = classify_target(t)
        t_emitted = apply_prefix_suffix(t, cfg, is_view=(kind=="VIEW"))
        if kind == "VIEW":
            views_sql.append(f"-- >>> {t_emitted}\n" + build_view_sql(t_emitted, rows))
        else:
            ddls_sql.append(f"-- >>> {t_emitted}\n" + iceberg_table_ddl(t_emitted, rows, cfg))
            if kind == "XREF":
                xref_sql.append(f"-- >>> {t_emitted}\n" + insert_sql(t_emitted, rows))
            else:
                fgac_sql.append(f"-- >>> {t_emitted}\n" + insert_sql(t_emitted, rows))

    (out_dir/"01_views.sql").write_text("\n\n".join(views_sql), encoding="utf-8")
    (out_dir/"02_tables.sql").write_text("\n\n".join(ddls_sql), encoding="utf-8")
    (out_dir/"03_xref_inserts.sql").write_text("\n\n".join(xref_sql), encoding="utf-8")
    (out_dir/"04_fgac_inserts.sql").write_text("\n\n".join(fgac_sql), encoding="utf-8")

    creates = [p.read_text(encoding="utf-8") for p in [out_dir/"01_views.sql", out_dir/"02_tables.sql"] if p.exists()]
    (out_dir/"90_all_create.sql").write_text("\n\n".join(creates), encoding="utf-8")
    inserts = [p.read_text(encoding="utf-8") for p in [out_dir/"03_xref_inserts.sql", out_dir/"04_fgac_inserts.sql"] if p.exists()]
    stmt = ("EXECUTE STATEMENT SET\nBEGIN\n\n" + "\n\n".join(inserts) + "\n\nEND;\n") if inserts else ""
    (out_dir/"95_all_inserts_stmtset.sql").write_text(stmt, encoding="utf-8")
    everything = []
    if creates: everything.append((out_dir/"90_all_create.sql").read_text(encoding="utf-8"))
    if stmt.strip(): everything.append((out_dir/"95_all_inserts_stmtset.sql").read_text(encoding="utf-8"))
    (out_dir/"99_everything.sql").write_text("\n\n".join(everything), encoding="utf-8")

    (out_dir/"validation_report.txt").write_text("\n".join(issues) if issues else "OK: no warnings", encoding="utf-8")
    return issues

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--sttm", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    issues = generate(Path(args.sttm), Path(args.out_dir))
    print("Issues:" if issues else "No issues found.")
    if issues:
        print("\n".join(issues))
    print("Outputs in:", args.out_dir)

if __name__ == "__main__":
    main()
