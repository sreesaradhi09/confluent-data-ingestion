#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import List, Dict
import pandas as pd
import re

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").map(lambda x: "" if str(x).strip().lower()=="nan" else str(x).strip())
    return df

def read_cfg(xl: pd.ExcelFile) -> pd.DataFrame:
    try:
        cfg = pd.read_excel(xl, sheet_name="Config", dtype=str).fillna("")
        cfg.columns = [str(c).strip().lower() for c in cfg.columns]
        for c in cfg.columns:
            cfg[c] = cfg[c].astype(str).fillna("").map(lambda x: "" if x.strip().lower()=="nan" else x.strip())
    except Exception:
        cfg = pd.DataFrame({"key":[], "value":[]})
    return cfg

def cfg_get(cfg: pd.DataFrame, key: str, default: str = "") -> str:
    if "key" not in cfg.columns or "value" not in cfg.columns:
        return default
    m = cfg[cfg["key"] == key]
    if m.empty:
        return default
    v = str(m["value"].iloc[0]).strip()
    return default if v.lower()=="nan" else v

def stage_rank(s: str) -> int:
    u = (s or "").upper()
    return {"VIEW":0,"XREF":1,"FGAC":2}.get(u, 99)

def apply_prefix_suffix(name: str, cfg: pd.DataFrame, is_view: bool) -> str:
    if is_view:
        pref = cfg_get(cfg, "view_prefix", "")
        suff = cfg_get(cfg, "view_suffix", "")
    else:
        pref = cfg_get(cfg, "table_prefix", "")
        suff = cfg_get(cfg, "table_suffix", "")
    return f"{pref}{name}{suff}"

def qident(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return s
    if s[0] in "`(":
        return s
    return f"`{s}`"

def choose_expr(row: dict, is_view: bool) -> str:
    override = (row.get("ExprOverride") or "").strip()
    stx = (row.get("SourceTransformExpr") or "").strip()
    tgt = (row.get("TargetDataType") or "STRING").strip() or "STRING"

    if is_view:
        if override:
            return override if re.match(r"(?is)^\s*CAST\s*\(", override) else f"CAST({override} AS {tgt})"
        if stx:
            return stx if re.match(r"(?is)^\s*CAST\s*\(", stx) else f"CAST({stx} AS {tgt})"
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

    if override:
        return override
    if stx:
        return stx
    sf = (row.get("SourceField") or "").strip()
    if sf:
        return sf
    return (row.get("TargetColumn") or "").strip() or "NULL"

def build_view_sql(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    selects = [f"  {choose_expr(r, True)} AS {r['TargetColumn']}" for r in rows if r.get("TargetColumn")]
    pk_filter = ""
    for r in rows:
        if (str(r.get("IsTargetPK",""))).upper()=="Y" and str(r.get("FilterPredicate","" )).strip():
            pk_filter = str(r["FilterPredicate"]).strip()
            break
    where = f"\nWHERE {pk_filter}" if pk_filter else ""
    src = next((f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}"
                for r in rows if r.get("SourcePrimaryTable","")), "")
    if not src:
        fallback = cfg_get(cfg, "raw_table_name", cfg_get(cfg, "default_source_table", "")).strip()
        if fallback:
            src = f"{qident(fallback)} t"
        else:
            src = "(VALUES(1)) t(dummy)"
    return f"CREATE VIEW {table} AS\nSELECT\n" + ",\n".join(selects) + f"\nFROM {src}{where};"

def kafka_table_ddl(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    seen = set(); col_lines = []
    pk = []
    for r in rows:
        c = r["TargetColumn"]; t = r["TargetDataType"]
        if c and t and c not in seen:
            col_lines.append(f"  {c} {t}")
            seen.add(c)
        if (str(r.get("IsTargetPK",""))).upper()=="Y" and c not in pk:
            pk.append(c)
    with_items = []
    if "key" in cfg.columns and "value" in cfg.columns:
        for _, row in cfg.iterrows():
            k = str(row.get("key","" )).strip(); v = str(row.get("value","" )).strip()
            if k.lower().startswith("with.") and v:
                with_items.append((k[5:], v))
    if table.upper().startswith("XREF_"):
        has_changelog = any(k == "changelog.mode" for k, _ in with_items)
        if not has_changelog:
            with_items.append(("changelog.mode", "upsert"))
    vp = cfg_get(cfg, "table_value_format", "avro-registry")
    props = [f"'value.format'='{vp}'"] + [f"'{k}'='{v}'" for k, v in with_items]
    if pk:
        col_lines.append("  " + f",PRIMARY KEY ({', '.join(pk)}) NOT ENFORCED")
    ddl = "CREATE TABLE IF NOT EXISTS " + table + " (\n" + ",\n".join(col_lines) + "\n)"
    if props:
        ddl += "\nWITH (\n  " + ", ".join(props) + "\n)"
    ddl += ";"
    return ddl

def join_clause(rows: List[dict]) -> str:
    for r in rows:
        jt = str(r.get("JoinTable","" )).strip()
        jc = str(r.get("JoinCondition","" )).strip()
        if jt and jc:
            jty = (str(r.get("JoinType","" )) or "LEFT").upper()
            if jty not in {"INNER","LEFT","RIGHT","FULL"}:
                jty = "LEFT"
            ja = str(r.get("JoinAlias","" )).strip() or "j"
            return f"\n  {jty} JOIN {jt} {ja} ON {jc}"
    return ""

def insert_sql(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    cols = [r["TargetColumn"] for r in rows if r.get("TargetColumn")]
    selects = [f"  {choose_expr(r, False)} AS {r['TargetColumn']}" for r in rows if r.get("TargetColumn")]
    drv = next((f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}" for r in rows if r.get("SourcePrimaryTable","")), "")
    if not drv:
        fb = cfg_get(cfg, "raw_table_name", cfg_get(cfg, "default_source_table", "")).strip()
        drv = f"{qident(fb)} t" if fb else "(VALUES(1)) t(dummy)"
    join = join_clause(rows)
    return f"INSERT INTO {table} ({', '.join(cols)})\nSELECT\n" + ",\n".join(selects) + f"\nFROM {drv}{join};"

def classify_target(name: str) -> str:
    u = (name or "").upper()
    if u.endswith("_VIEW") or u.endswith(" VIEW"):
        return "VIEW"
    if u.startswith("XREF_"):
        return "XREF"
    if u.startswith("FGAC_"):
        return "FGAC"
    return "FGAC"

def validate_views(df: pd.DataFrame) -> List[str]:
    issues = []
    views = df[df["PipelineStage"].str.upper()=="VIEW"]
    for i, r in views.iterrows():
        override = str(r.get("ExprOverride","" )).strip()
        mf = str(r.get("MessageFormat","" )).upper()
        key = str(r.get("FieldSelector","" )).strip()
        src = str(r.get("SourceField","" )).strip()
        if not src:
            issues.append(f"WARN row {i}: View SourceField (payload column) is empty")
        if not override and mf=="JSON" and not key:
            issues.append(f"WARN row {i}: JSON View missing FieldSelector (JSON key)")
        if mf=="JSON" and key and key.startswith("$"):
            issues.append(f"WARN row {i}: FieldSelector must not start with '$'")
    return issues

def generate(sttm_path: Path, out_dir: Path):
    xl = pd.ExcelFile(sttm_path)
    cfg = read_cfg(xl)
    sheet_name = "STTM_Mapping" if "STTM_Mapping" in xl.sheet_names else ("STTM" if "STTM" in xl.sheet_names else xl.sheet_names[0])
    df = norm_cols(pd.read_excel(xl, sheet_name=sheet_name, dtype=str).fillna(""))
    if "PipelineStage" not in df.columns and "pipelinestage" in [c.lower() for c in df.columns]:
        for c in df.columns:
            if c.lower()=="pipelinestage":
                df = df.rename(columns={c:"PipelineStage"})
                break
    df["_s"] = df["PipelineStage"].apply(stage_rank)
    df["_p"] = df.get("IsTargetPK", "").apply(lambda x: 0 if str(x).upper()=="Y" else 1)
    df = df.sort_values(by=["_s","TargetTable","_p","TargetColumn"], na_position="last").drop(columns=["_s","_p"], errors="ignore")
    issues = validate_views(df)
    grouped: Dict[str, List[dict]] = {}
    for _, r in df.iterrows():
        row = {c: str(r.get(c, "")).strip() for c in df.columns}
        t = row.get("TargetTable","")
        if not t:
            continue
        grouped.setdefault(t, []).append(row)

    views_sql, ddls_sql, xref_sql, fgac_sql = [], [], [], []
    for t, rows in grouped.items():
        kind = classify_target(t)
        t_emitted = apply_prefix_suffix(t, cfg, is_view=(kind=="VIEW"))
        if kind == "VIEW":
            views_sql.append(f"-- >>> {t_emitted}\n" + build_view_sql(t_emitted, rows, cfg))
        else:
            ddls_sql.append(f"-- >>> {t_emitted}\n" + kafka_table_ddl(t_emitted, rows, cfg))
            if kind == "XREF":
                xref_sql.append(f"-- >>> {t_emitted}\n" + insert_sql(t_emitted, rows, cfg))
            else:
                fgac_sql.append(f"-- >>> {t_emitted}\n" + insert_sql(t_emitted, rows, cfg))

    out_dir.mkdir(parents=True, exist_ok=True)
    sections = []
    if views_sql:
        sections.append("-- ===== VIEWS =====\n" + "\n\n".join(views_sql).strip())
    if ddls_sql:
        sections.append("-- ===== TABLES (Kafka + Avro) =====\n" + "\n\n".join(ddls_sql).strip())
    if xref_sql or fgac_sql:
        body_parts = []
        if xref_sql:
            body_parts.append("\n\n".join(xref_sql).strip())
        if fgac_sql:
            body_parts.append("\n\n".join(fgac_sql).strip())
        stmtset = "EXECUTE STATEMENT SET\nBEGIN\n\n" + ("\n\n".join(body_parts)) + "\n\nEND;"
        sections.append("-- ===== INSERT STATEMENT SET =====\n" + stmtset)
    all_sql = ("\n\n".join(sections)).strip() + "\n"
    (out_dir / "all_statement_set.sql").write_text(all_sql, encoding="utf-8")
    return issues, all_sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sttm", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    issues, _ = generate(Path(args.sttm), Path(args.out_dir))
    print("[done] OK" if not issues else "\n".join(issues))

if __name__ == "__main__":
    main()
