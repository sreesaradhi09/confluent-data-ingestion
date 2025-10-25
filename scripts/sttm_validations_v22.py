#!/usr/bin/env python3
# sttm_validations_v22.py
# Matrix-aware validations for STTM v22 (no global config; per-table only via Config_TableMatrix)

from typing import Dict, List, Tuple
from pathlib import Path
import pandas as pd
import re

# -------- Utilities --------

def _u(s: str) -> str:
    return (s or "").strip().upper()

def _is_int(s: str) -> bool:
    return bool(re.match(r"^\d+$", (s or "").strip()))

def _uniq(seq):
    seen = set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            yield x

# -------- Matrix loader --------

def load_table_matrix(xl: pd.ExcelFile) -> Tuple[Dict[str, Dict[str, str]], List[str], pd.DataFrame]:
    """Load Config_TableMatrix -> (per_table_props, table_columns, raw_df)."""
    if 'Config_TableMatrix' not in xl.sheet_names:
        return {}, [], pd.DataFrame()
    try:
        df = pd.read_excel(xl, sheet_name='Config_TableMatrix', dtype=str)
    except Exception:
        return {}, [], pd.DataFrame()

    if df is None or df.empty or not hasattr(df, "columns"):
        return {}, [], pd.DataFrame()

    # normalize headers (case-insensitive, trimmed)
    df.columns = [str(c).strip() for c in df.columns if str(c).strip()]
    if not any(str(c).strip().lower() == 'key' for c in df.columns):
        return {}, [], df  # invalid layout (no Key column)

    # rename the key column for internal consistency
    for c in list(df.columns):
        if str(c).strip().lower() == 'key':
            df = df.rename(columns={c: 'Key'})
            break

    table_cols = [c for c in df.columns if c != 'Key']
    per_table: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        key = (row.get('Key') or '').strip()
        if not key:
            continue
        for tcol in table_cols:
            val = (row.get(tcol, '') or '').strip()
            if not val or val.lower() in {'na','n/a','none'}:
                continue
            per_table.setdefault(tcol, {})[key] = val
    return per_table, table_cols, df

# -------- Mapping normalizer --------

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).fillna('').map(lambda x: '' if str(x).strip().lower()=='nan' else str(x).strip())
    return df

# -------- Core validations --------

def validate_views_and_alignment(mapping_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Stage-agnostic checks that do not require the matrix."""
    errors, warns = [], []
    df = norm_cols(mapping_df)
    # Required columns
    req = ['TargetTable','TargetColumn','PipelineStage']
    for r in req:
        if r not in df.columns:
            errors.append(f"Missing required column in mapping: {r}")
            return {"errors": errors, "warnings": warns}
    # Group validations
    for tname, tdf in df.groupby("TargetTable"):
        if not str(tname).strip():
            errors.append("Found row with blank TargetTable.")
            continue
        stage = _u(str(tdf["PipelineStage"].iloc[0]))
        rows = [{c: str(r.get(c,'')) for c in tdf.columns} for _, r in tdf.iterrows()]
        # must have columns
        tgt_cols = [r.get("TargetColumn","") for r in rows if r.get("TargetColumn","")]
        if not tgt_cols:
            errors.append(f"[{tname}] has no TargetColumn entries.")
        # duplicates
        dup = [c for c in tgt_cols if tgt_cols.count(c) > 1]
        for c in _uniq(dup):
            errors.append(f"[{tname}] duplicate TargetColumn: {c}")
        # PK
        pk_cols = [r["TargetColumn"] for r in rows if _u(r.get("IsTargetPK",""))=='Y' and r.get("TargetColumn")]
        if len(pk_cols) != len(set(pk_cols)):
            warns.append(f"[{tname}] duplicate PK marks on: {', '.join(pk_cols)}")
        # driving table present
        spts = [r.get("SourcePrimaryTable","") for r in rows if r.get("SourcePrimaryTable","")]
        if not spts:
            errors.append(f"[{tname}] missing SourcePrimaryTable (at least one row must specify it).")
        elif stage == 'VIEW' and len(list(_uniq(spts))) > 1:
            warns.append(f"[{tname}] VIEW uses multiple SourcePrimaryTable values: {list(_uniq(spts))}")
        # VIEW-specific
        if stage == 'VIEW':
            for i, r in enumerate(rows, start=1):
                mf = _u(r.get("MessageFormat",""))
                ov = r.get("ExprOverride","").strip()
                st = r.get("SourceTransformExpr","").strip()
                sf = r.get("SourceField","").strip()
                fsel = r.get("FieldSelector","").strip()
                if mf and mf not in {"JSON","CSV"}:
                    errors.append(f"[{tname}] row#{i} invalid MessageFormat: {mf}")
                if mf == "JSON":
                    if not ov and not st and not (sf or fsel):
                        errors.append(f"[{tname}] row#{i} JSON View missing key (SourceField or FieldSelector).")
                    if (sf or fsel).startswith("$"):
                        errors.append(f"[{tname}] row#{i} JSON key must not start with '$'.")
                if mf == "CSV":
                    if not ov and not st and fsel and not _is_int(fsel):
                        errors.append(f"[{tname}] row#{i} CSV FieldSelector must be numeric when provided. Got: {fsel}")
            # FilterPredicate shape
            fps = [r.get("FilterPredicate","").strip() for r in rows if _u(r.get("IsTargetPK",""))=='Y' and r.get("FilterPredicate","").strip()]
            if fps:
                raw = fps[0]
                if re.match(r"^\s*(WHERE|AND|OR)\b", raw, flags=re.IGNORECASE):
                    warns.append(f"[{tname}] FilterPredicate should be condition only; drop leading WHERE/AND/OR.")
        else:
            # Join completeness
            jt_vals = [r.get("JoinTable","").strip() for r in rows if r.get("JoinTable","").strip()]
            jc_vals = [r.get("JoinCondition","").strip() for r in rows if r.get("JoinCondition","").strip()]
            if jt_vals and not jc_vals:
                warns.append(f"[{tname}] JoinTable specified but JoinCondition missing.")
            if jc_vals and not jt_vals:
                errors.append(f"[{tname}] JoinCondition provided but JoinTable empty.")
    return {"errors": errors, "warnings": warns}

def validate_against_matrix(mapping_df: pd.DataFrame, matrix_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Matrix-aware checks: every mapping table must appear; XREF must set upsert; matrix columns unused get WARN."""
    errors, warns = [], []
    if "TargetTable" not in mapping_df.columns:
        errors.append("Missing TargetTable column in mapping.")
        return {"errors": errors, "warnings": warns}
    mapping_tables = set(str(x).strip() for x in mapping_df["TargetTable"].tolist() if str(x).strip())

    if matrix_df is None or matrix_df.empty:
        errors.append("Config_TableMatrix sheet missing or empty.")
        return {"errors": errors, "warnings": warns}

    df = matrix_df.copy()
    # normalize headers and ensure we have a Key column
    df.columns = [str(c).strip() for c in df.columns if str(c).strip()]
    if not any(str(c).strip().lower() == 'key' for c in df.columns):
        errors.append("Config_TableMatrix must contain a 'Key' column (any case).")
        return {"errors": errors, "warnings": warns}
    for c in list(df.columns):
        if str(c).strip().lower() == 'key':
            df = df.rename(columns={c: 'Key'})
            break

    table_cols = [c for c in df.columns if c != 'Key']

    # Build per-table map
    per_table: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        key = (row.get('Key') or '').strip()
        if not key:
            continue
        for tcol in table_cols:
            val = (row.get(tcol, "") or "").strip()
            if not val or val.lower() in {"na","n/a","none"}:
                continue
            per_table.setdefault(tcol, {})[key] = val

    # 1) presence for each mapping table
    for t in sorted(mapping_tables):
        props = per_table.get(t, {})
        if not props:
            errors.append(f"[Config_TableMatrix] Missing per-table properties for mapping TargetTable '{t}'.")
        # 2) XREF must have upsert
        if t.upper().startswith("XREF_"):
            cm = (props.get("changelog.mode","")).strip().lower()
            if cm != "upsert":
                errors.append(f"[Config_TableMatrix] XREF table '{t}' must set changelog.mode=upsert (found '{cm or 'missing'}').")

    # 3) warn matrix-only columns
    for tcol in table_cols:
        if tcol not in mapping_tables:
            warns.append(f"[Config_TableMatrix] Column '{tcol}' not found in mapping TargetTable list (assuming external/pre-existing).")

    # 4) warn duplicates within same table col (last-write-wins)
    for tcol in table_cols:
        keys = [ (row.get('Key') or '').strip() for _, row in df.iterrows()
                 if (row.get(tcol, "") or "").strip() and str(row.get(tcol)).lower() not in {"na","n/a","none"} ]
        if len(keys) != len(set(keys)):
            warns.append(f"[Config_TableMatrix] Duplicate keys detected for table column '{tcol}' (last value will win).")

    return {"errors": errors, "warnings": warns}

# -------- Reporter --------

def write_issues_csv(out_dir: Path, issues: Dict[str, List[str]]):
    rows = []
    for e in issues.get("errors", []):
        rows.append({"level":"ERROR", "message": e})
    for w in issues.get("warnings", []):
        rows.append({"level":"WARN", "message": w})
    if not rows:
        rows.append({"level":"INFO", "message":"No issues found"})
    pd.DataFrame(rows).to_csv(Path(out_dir) / "issues_v22.csv", index=False)
