#!/usr/bin/env python3
# sttm_to_flink_v22.py
# - Config_TableMatrix only (no global config)
# - XREF upsert must come from matrix (validator enforces)
# - Consolidated SQL: Views -> Tables -> EXECUTE STATEMENT SET (XREF + FGAC)
# - FilterPredicate support:
#     * VIEW: take the first PK row's FilterPredicate, rewrite bare tokens as JSON_VALUE(...)
#     * XREF/FGAC: AND all non-empty FilterPredicate rows (after stripping leading WHERE/AND/OR)
# - Expression rules:
#     * Views: ExprOverride > SourceTransformExpr > (JSON/CSV auto) + CAST to TargetDataType
#     * Non-views: no auto-CAST unless provided via ExprOverride/SourceTransformExpr

import argparse
from pathlib import Path
from typing import List, Dict
import pandas as pd
import re

from sttm_validations_v22 import (
    load_table_matrix,
    validate_views_and_alignment,
    validate_against_matrix,
    write_issues_csv,
)

# -------- Helpers --------

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).fillna('').map(lambda x: '' if str(x).strip().lower() == 'nan' else str(x).strip())
    return df

def qident(name: str) -> str:
    s = (name or '').strip()
    if not s:
        return s
    if s[0] in '`(':
        return s
    return f'`{s}`'

# -------- Predicate handling --------

_SQL_RESERVED = {
    "LIKE","AND","OR","NOT","IN","BETWEEN","IS","NULL","EXISTS","ALL","ANY","SOME",
    "TRUE","FALSE","CASE","WHEN","THEN","ELSE","END","ON","AS","JOIN","LEFT","RIGHT",
    "FULL","INNER","OUTER","GROUP","BY","ORDER","HAVING","DISTINCT","ASC","DESC",
    "LIMIT","OFFSET"
}
_JSON_FIELD_TOKEN = re.compile(r"\b([A-Z][A-Z0-9_]*[A-Z0-9])\b")

def sanitize_predicate(raw: str) -> str:
    """
    Remove a leading WHERE/AND/OR and trailing semicolons.
    Does NOT rewrite tokens; safe for XREF/FGAC which should use fields as-is.
    """
    s = (raw or "").strip()
    s = re.sub(r'^\s*(WHERE|AND|OR)\b', '', s, flags=re.IGNORECASE).strip()
    s = re.sub(r';+\s*$', '', s)
    return s

def _rewrite_token(token: str, payload_col: str) -> str:
    if token in _SQL_RESERVED:
        return token
    if token.isdigit():
        return token
    if "_" not in token and len(token) <= 3:
        return token
    return f"JSON_VALUE(CAST({payload_col} AS STRING), '$.{token}')"

def rewrite_predicate_as_json(fp: str, payload_col: str) -> str:
    """
    For VIEW filters only: rewrite bare field-like tokens to JSON_VALUE(... '$.<field>')
    Leaves quoted strings intact.
    """
    if not fp or "JSON_VALUE" in fp.upper():
        return fp
    out = []
    i, n = 0, len(fp)
    in_s = in_d = False
    while i < n:
        ch = fp[i]
        if ch == "'" and not in_d:
            out.append(ch); i += 1; in_s = not in_s; continue
        if ch == '"' and not in_s:
            out.append(ch); i += 1; in_d = not in_d; continue
        if in_s or in_d:
            out.append(ch); i += 1; continue
        m = _JSON_FIELD_TOKEN.match(fp, i)
        if m:
            token = m.group(1)
            out.append(_rewrite_token(token, payload_col)); i = m.end()
        else:
            out.append(ch); i += 1
    return "".join(out)

# -------- Expression builder --------

def choose_expr(row: dict, is_view: bool, raw_payload_col: str, csv_delim: str, auto_csv_index: Dict[str, int]) -> str:
    override = (row.get('ExprOverride') or '').strip()
    stx = (row.get('SourceTransformExpr') or '').strip()
    tgt = (row.get('TargetDataType') or 'STRING').strip() or 'STRING'

    if is_view:
        if override:
            return override if re.match(r'(?is)^\s*CAST\s*\(', override) else f'CAST({override} AS {tgt})'
        if stx:
            return stx if re.match(r'(?is)^\s*CAST\s*\(', stx) else f'CAST({stx} AS {tgt})'

        mf   = (row.get('MessageFormat') or '').upper()
        sfld = (row.get('SourceField') or '').strip()
        fsel = (row.get('FieldSelector') or '').strip()

        if mf == 'JSON':
            key = sfld or fsel
            base = f"JSON_VALUE(CAST({raw_payload_col} AS STRING), '$.{key}')"
        elif mf == 'CSV':
            srcp = sfld if sfld else raw_payload_col
            if re.match(r'^\d+$', fsel or ''):
                idx = int(fsel)
            else:
                idx = int(auto_csv_index.get(row.get('TargetColumn'), 0))
            base = f"SPLIT_INDEX(CAST({srcp} AS STRING), '{csv_delim}', {idx})"
        else:
            base = sfld or raw_payload_col

        norm = f"TRIM({base})" if tgt.upper().startswith('STRING') else f"NULLIF(TRIM({base}), '')"
        return f"CAST({norm} AS {tgt})"

    # Non-views: do not auto-cast/parse unless explicitly provided
    if override:
        return override
    if stx:
        return stx
    sf = (row.get('SourceField') or '').strip()
    if sf:
        return sf
    return (row.get('TargetColumn') or '').strip() or 'NULL'

# -------- Table DDL props (matrix-based) --------

def resolve_table_props(table_logical: str, table_emitted: str, matrix_df: pd.DataFrame) -> Dict[str, str]:
    """
    Reads per-table WITH(...) options from Config_TableMatrix.
    - Robust to header case/whitespace
    - Prefers logical table column; falls back to emitted table column
    - Skips blank/na/n/a/none values
    - Expands ${table_name} with emitted name
    """
    if matrix_df is None or not hasattr(matrix_df, "empty") or matrix_df.empty:
        return {}

    # Normalize headers (trim), ensure a 'Key' column exists (case-insensitive)
    df = matrix_df.copy()
    df.columns = [str(c).strip() for c in df.columns if str(c).strip()]
    key_col = next((c for c in df.columns if str(c).strip().lower() == "key"), None)
    if not key_col:
        return {}
    if key_col != "Key":
        df = df.rename(columns={key_col: "Key"})

    # Build mapping of normalized table header -> original header
    table_headers = {str(c).strip(): c for c in df.columns if c != "Key"}

    # Prefer the logical table name, then try the emitted name
    colname = None
    if table_logical in table_headers:
        colname = table_headers[table_logical]
    elif table_emitted in table_headers:
        colname = table_headers[table_emitted]
    else:
        return {}

    props: Dict[str, str] = {}
    for _, row in df.iterrows():
        key = (row.get("Key") or "").strip()
        if not key:
            continue
        val = (row.get(colname, "") or "").strip()
        if not val or val.lower() in {"na", "n/a", "none"}:
            continue
        # Macro expansion
        val = val.replace("${table_name}", table_emitted)
        props[key] = val
    return props

# -------- SQL builders --------

def build_view_sql(table_emitted: str, rows: List[dict], raw_payload_col: str, filter_predicate: str) -> str:
    selects = [f"  {row['__expr__']} AS {row['TargetColumn']}" for row in rows if row.get('TargetColumn')]
    src = next(
        (f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}"
         for r in rows if r.get("SourcePrimaryTable","")),
        ""
    )
    if not src:
        src = "(VALUES(1)) t(dummy)"
    where = f"\nWHERE {filter_predicate}" if filter_predicate else ""
    return f"CREATE VIEW {table_emitted} AS\nSELECT\n" + ",\n".join(selects) + f"\nFROM {src}{where};"

def build_table_ddl(table_emitted: str, rows: List[dict], props: Dict[str, str]) -> str:
    """
    Emit CREATE TABLE DDL from mapping rows and per-table WITH(...) props.
    - Columns come from TargetColumn + TargetDataType (deduped by name, first wins)
    - PRIMARY KEY built from rows where IsTargetPK == 'Y' (NOT ENFORCED)
    - WITH(...) key/values come from Config_TableMatrix (already resolved -> props)
    """
    cols_seen: set = set()
    col_lines: List[str] = []
    pk_cols: List[str] = []

    for r in rows:
        c = (r.get('TargetColumn') or '').strip()
        t = (r.get('TargetDataType') or 'STRING').strip() or 'STRING'
        if c and c not in cols_seen:
            col_lines.append(f"  {c} {t}")
            cols_seen.add(c)
        if c and (r.get('IsTargetPK','').strip().upper() == 'Y') and c not in pk_cols:
            pk_cols.append(c)

    if pk_cols:
        col_lines.append("  " + f"PRIMARY KEY ({', '.join(pk_cols)}) NOT ENFORCED")

    ddl = f"CREATE TABLE IF NOT EXISTS {table_emitted} (\n" + ",\n".join(col_lines) + "\n)"
    if props:
        kv = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
        ddl += f"\nWITH (\n  {kv}\n)"
    ddl += ";"
    return ddl

def build_insert_sql(table_emitted: str, rows: List[dict], where_predicate: str = '') -> str:
    cols = [r['TargetColumn'] for r in rows if r.get('TargetColumn')]
    selects = [f"  {r['__expr__']} AS {r['TargetColumn']}" for r in rows if r.get('TargetColumn')]
    drv = next((f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}" for r in rows if r.get('SourcePrimaryTable','')), '')
    if not drv:
        drv = '(VALUES(1)) t(dummy)'
    # one join max from first row that has both JoinTable & JoinCondition
    join = ''
    for r in rows:
        jt = r.get('JoinTable','').strip(); jc = r.get('JoinCondition','').strip()
        if jt and jc:
            jty = (r.get('JoinType','') or 'LEFT').upper()
            jty = jty if jty in {'INNER','LEFT','RIGHT','FULL'} else 'LEFT'
            ja = (r.get('JoinAlias','') or 'j').strip()
            join = f'\n  {jty} JOIN {jt} {ja} ON {jc}'
            break
    where_sql = f"\nWHERE {where_predicate}" if where_predicate else ''
    return 'INSERT INTO ' + table_emitted + ' (' + ', '.join(cols) + ')\nSELECT\n' + ',\n'.join(selects) + f'\nFROM {drv}{join}{where_sql};'

# -------- Main generator --------

def generate(sttm_path: Path, out_dir: Path):
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    xl = pd.ExcelFile(sttm_path)
    # mapping
    sheet_name = 'STTM_Mapping' if 'STTM_Mapping' in xl.sheet_names else ('STTM' if 'STTM' in xl.sheet_names else xl.sheet_names[0])
    mapping = norm_cols(pd.read_excel(xl, sheet_name=sheet_name, dtype=str).fillna(''))
    # config matrix
    per_table_props, matrix_tables, matrix_df = load_table_matrix(xl)

    # validations
    v1 = validate_views_and_alignment(mapping)
    v2 = validate_against_matrix(mapping, matrix_df)
    issues = {"errors": v1["errors"] + v2["errors"], "warnings": v1["warnings"] + v2["warnings"]}
    write_issues_csv(out_dir, issues)

    # sort rows for stable output
    mapping['_s'] = mapping['PipelineStage'].apply(lambda x: {'VIEW':0,'XREF':1,'FGAC':2}.get((x or '').upper(), 99))
    mapping['_p'] = mapping.get('IsTargetPK','').apply(lambda x: 0 if str(x).upper()=='Y' else 1)
    mapping = mapping.sort_values(by=['_s','TargetTable','_p','TargetColumn'], na_position='last').drop(columns=['_s','_p'], errors='ignore')

    # constants
    raw_payload_col = 'val'   # default source payload column in Kafka schema topic
    csv_delim = ','

    # group by target table
    grouped: Dict[str, List[dict]] = {}
    for _, r in mapping.iterrows():
        row = {c: str(r.get(c, '')).strip() for c in mapping.columns}
        t = row.get('TargetTable','')
        if not t:
            continue
        grouped.setdefault(t, []).append(row)

    views_sql, ddls_sql, xref_inserts, fgac_inserts = [], [], [], []

    for logical, rows in grouped.items():
        stage = (rows[0].get('PipelineStage','FGAC') or 'FGAC').upper()
        is_view = stage == 'VIEW'
        emitted = logical  # v22: no prefix/suffix

        # build expressions and CSV auto index for views
        auto_idx: Dict[str, int] = {}
        if is_view:
            reserved = set()
            for r in rows:
                if (r.get('MessageFormat','').upper() != 'CSV') or r.get('ExprOverride','').strip() or r.get('SourceTransformExpr','').strip():
                    continue
                fsel = (r.get('FieldSelector') or '').strip()
                if re.match(r'^\d+$', fsel or ''):
                    reserved.add(int(fsel))
            def next_free(start: int) -> int:
                i = start
                while i in reserved:
                    i += 1
                return i
            cursor = 0
            for r in rows:
                if (r.get('MessageFormat','').upper() != 'CSV') or r.get('ExprOverride','').strip() or r.get('SourceTransformExpr','').strip():
                    continue
                fsel = (r.get('FieldSelector') or '').strip()
                if re.match(r'^\d+$', fsel or ''):
                    cursor = max(cursor, int(fsel) + 1)
                else:
                    idx = next_free(cursor)
                    auto_idx[r.get('TargetColumn')] = idx
                    reserved.add(idx)
                    cursor = idx + 1

        # compute expressions for each row
        for r in rows:
            r['__expr__'] = choose_expr(r, is_view, raw_payload_col, csv_delim, auto_idx)

        # Filter predicate
        if is_view:
            pk_filter = ""
            for r in rows:
                if (r.get("IsTargetPK","") or '').upper()=="Y" and (r.get("FilterPredicate","") or '').strip():
                    pk_filter = r["FilterPredicate"].strip(); break
            filt_sql = rewrite_predicate_as_json(sanitize_predicate(pk_filter), raw_payload_col) if pk_filter else ""
            views_sql.append(f'-- >>> {emitted}\n' + build_view_sql(emitted, rows, raw_payload_col, filt_sql))
        else:
            # Non-views: combine all row FilterPredicate with AND (as-is; only sanitize leading WHERE/AND/OR)
            preds: List[str] = []
            seen = set()
            for r in rows:
                fp = (r.get('FilterPredicate','') or '').strip()
                if not fp:
                    continue
                clean = sanitize_predicate(fp)
                if clean and clean not in seen:
                    preds.append(clean); seen.add(clean)
            where_nonview = ' AND '.join(preds)

            # DDL props from matrix
            props = resolve_table_props(logical, emitted, matrix_df)
            ddls_sql.append(f'-- >>> {emitted}\n' + build_table_ddl(emitted, rows, props))
            if stage == 'XREF':
                xref_inserts.append(f'-- >>> {emitted}\n' + build_insert_sql(emitted, rows, where_nonview))
            else:
                fgac_inserts.append(f'-- >>> {emitted}\n' + build_insert_sql(emitted, rows, where_nonview))

    sections = []
    if views_sql:
        sections.append('-- ===== VIEWS =====\n' + '\n\n'.join(views_sql).strip())
    if ddls_sql:
        sections.append('-- ===== TABLES (Kafka + Avro) =====\n' + '\n\n'.join(ddls_sql).strip())
    if xref_inserts or fgac_inserts:
        parts = []
        if xref_inserts:
            parts.append('\n\n'.join(xref_inserts).strip())
        if fgac_inserts:
            parts.append('\n\n'.join(fgac_inserts).strip())
        stmtset = 'EXECUTE STATEMENT SET\nBEGIN\n\n' + ('\n\n'.join(parts)) + '\n\nEND;'
        sections.append('-- ===== INSERT STATEMENT SET =====\n' + stmtset)
    all_sql = ('\n\n'.join(sections)).strip() + '\n'
    (Path(out_dir) / '00_all.sql').write_text(all_sql, encoding='utf-8')
    return issues, all_sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sttm', required=True, help='Path to STTM workbook (xlsx)')
    ap.add_argument('--out-dir', required=True, help='Output directory for consolidated SQL')
    ap.add_argument('--fail-on-error', action='store_true', help='Exit non-zero if validation errors are found')
    args = ap.parse_args()
    issues, _ = generate(Path(args.sttm), Path(args.out_dir))
    errs = issues.get("errors", []); warns = issues.get("warnings", [])
    if errs:
        print("ERRORS:"); [print(" -", e) for e in errs]
        print("See issues_v22.csv")
        if args.fail_on_error: raise SystemExit(2)
    if warns:
        print("WARNINGS:"); [print(" -", w) for w in warns]
        print("See issues_v22.csv")
    if not errs and not warns:
        print("[done] OK (no validation issues).")

if __name__ == '__main__':
    main()
