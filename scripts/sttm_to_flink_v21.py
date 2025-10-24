#!/usr/bin/env python3
# sttm_to_flink_v21.py (imports validations from sttm_validations.py)

import argparse
from pathlib import Path
from typing import List, Dict
import pandas as pd
import re

from sttm_validations import (
    validate_views_basic,
    validate_alignment,
    write_issues_csv,
)

# ---------- Basic helpers ----------

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).fillna('').map(lambda x: '' if str(x).strip().lower()=='nan' else str(x).strip())
    return df

def read_cfg(xl: pd.ExcelFile) -> pd.DataFrame:
    try:
        cfg = pd.read_excel(xl, sheet_name='Config', dtype=str).fillna('')
        cfg.columns = [str(c).strip().lower() for c in cfg.columns]
        for c in cfg.columns:
            cfg[c] = cfg[c].astype(str).fillna('').map(lambda x: '' if x.strip().lower()=='nan' else x.strip())
    except Exception:
        cfg = pd.DataFrame({'key':[], 'value':[]})
    return cfg

def cfg_get(cfg: pd.DataFrame, key: str, default: str = '') -> str:
    if 'key' not in cfg.columns or 'value' not in cfg.columns:
        return default
    m = cfg[cfg['key'] == key]
    if m.empty:
        return default
    v = str(m['value'].iloc[0]).strip()
    return default if v.lower()=='nan' else v

def stage_rank(s: str) -> int:
    u = (s or '').upper()
    return {'VIEW':0,'XREF':1,'FGAC':2}.get(u, 99)

def apply_prefix_suffix(name: str, cfg: pd.DataFrame, is_view: bool) -> str:
    if is_view:
        pref = cfg_get(cfg, 'view_prefix', '')
        suff = cfg_get(cfg, 'view_suffix', '')
    else:
        pref = cfg_get(cfg, 'table_prefix', '')
        suff = cfg_get(cfg, 'table_suffix', '')
    return f'{pref}{name}{suff}'

def qident(name: str) -> str:
    s = (name or '').strip()
    if not s:
        return s
    if s[0] in '`(':
        return s
    return f'`{s}`'

# ---------- Predicate handling ----------

_SQL_RESERVED = {
    "LIKE","AND","OR","NOT","IN","BETWEEN","IS","NULL","EXISTS","ALL","ANY","SOME",
    "TRUE","FALSE","CASE","WHEN","THEN","ELSE","END","ON","AS","JOIN","LEFT","RIGHT",
    "FULL","INNER","OUTER","GROUP","BY","ORDER","HAVING","DISTINCT","ASC","DESC",
    "LIMIT","OFFSET"
}

_JSON_FIELD_TOKEN = re.compile(r"\b([A-Z][A-Z0-9_]*[A-Z0-9])\b")

def sanitize_predicate(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r'^\s*(WHERE|AND|OR)\b', '', s, flags=re.IGNORECASE).strip()
    s = re.sub(r';+\s*$', '', s)
    return s

def _rewrite_token(token: str, payload_col: str) -> str:
    if token in _SQL_RESERVED:  # keep SQL operators intact
        return token
    if token.isdigit():
        return token
    if "_" not in token and len(token) <= 3:
        return token
    return f"JSON_VALUE(CAST({payload_col} AS STRING), '$.{token}')"

def rewrite_predicate_as_json(fp: str, payload_col: str) -> str:
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

# ---------- Expression builder ----------

def choose_expr(row: dict, is_view: bool) -> str:
    override = (row.get('ExprOverride') or '').strip()
    stx = (row.get('SourceTransformExpr') or '').strip()
    tgt = (row.get('TargetDataType') or 'STRING').strip() or 'STRING'

    def cfg_payload_default():
        try:
            if 'cfg_ref' in globals():
                rv = cfg_get(cfg_ref, 'raw_value_column', 'val')
                return rv or 'val'
        except Exception:
            pass
        return 'val'

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
            base = f"JSON_VALUE(CAST({cfg_payload_default()} AS STRING), '$.{key}')"
        elif mf == 'CSV':
            delim = ','
            try:
                if 'cfg_ref' in globals() and 'key' in cfg_ref.columns and 'value' in cfg_ref.columns:
                    sel = cfg_ref[cfg_ref['key']=='csv_delimiter']
                    if not sel.empty:
                        delim = str(sel['value'].iloc[0] or ',')
            except Exception:
                delim = ','
            srcp = sfld if sfld else cfg_payload_default()
            if re.match(r'^\d+$', fsel or ''):
                idx = fsel
            else:
                idx = CSV_AUTO_INDEX.get(row.get('TargetColumn'), '0') if 'CSV_AUTO_INDEX' in globals() else '0'
            base = f"SPLIT_INDEX(CAST({srcp} AS STRING), '{delim}', {idx})"
        else:
            base = sfld or cfg_payload_default()

        norm = f"TRIM({base})" if tgt.upper().startswith('STRING') else f"NULLIF(TRIM({base}), '')"
        return f"CAST({norm} AS {tgt})"

    # Non-views: no auto JSON/CSV or casts unless explicitly provided
    if override:
        return override
    if stx:
        return stx
    sf = (row.get('SourceField') or '').strip()
    if sf:
        return sf
    return (row.get('TargetColumn') or '').strip() or 'NULL'

# ---------- SQL builders ----------

def build_view_sql(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    global cfg_ref; cfg_ref = cfg
    global CSV_AUTO_INDEX
    CSV_AUTO_INDEX = {}
    reserved = set()

    # Reserve explicit indices
    for _r in rows:
        if (_r.get('MessageFormat') or '').upper() != 'CSV':
            continue
        if (_r.get('ExprOverride') or '').strip() or (_r.get('SourceTransformExpr') or '').strip():
            continue
        fsel = (_r.get('FieldSelector') or '').strip()
        if re.match(r'^\d+$', fsel or ''):
            reserved.add(int(fsel))

    def next_free(start: int) -> int:
        i = start
        while i in reserved:
            i += 1
        return i

    # Assign auto indices
    cursor = 0
    for _r in rows:
        if (_r.get('MessageFormat') or '').upper() != 'CSV':
            continue
        if (_r.get('ExprOverride') or '').strip() or (_r.get('SourceTransformExpr') or '').strip():
            continue
        fsel = (_r.get('FieldSelector') or '').strip()
        if re.match(r'^\d+$', fsel or ''):
            cursor = max(cursor, int(fsel) + 1)
        else:
            idx = next_free(cursor)
            CSV_AUTO_INDEX[_r.get('TargetColumn')] = str(idx)
            reserved.add(idx)
            cursor = idx + 1

    selects = [f"  {choose_expr(r, True)} AS {r['TargetColumn']}" for r in rows if r.get('TargetColumn')]

    # Filter predicate (first PK row only)
    pk_filter = ""
    for r in rows:
        if (str(r.get("IsTargetPK","")).upper()=="Y") and str(r.get("FilterPredicate","")).strip():
            pk_filter = str(r["FilterPredicate"]).strip()
            break

    where = ""
    if pk_filter:
        pred = sanitize_predicate(pk_filter)
        payload = cfg_get(cfg, "raw_value_column", "val") or "val"
        json_pred = rewrite_predicate_as_json(pred, payload)
        where = f"\nWHERE {json_pred}"

    src = next(
        (f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}"
         for r in rows if r.get("SourcePrimaryTable","")),
        ""
    )
    if not src:
        fallback = cfg_get(cfg, "raw_table_name", cfg_get(cfg, "default_source_table", "")).strip()
        src = f"{qident(fallback)} t" if fallback else "(VALUES(1)) t(dummy)"

    return f"CREATE VIEW {table} AS\nSELECT\n" + ",\n".join(selects) + f"\nFROM {src}{where};"

def iceberg_table_ddl(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    seen = set(); col_lines = []
    pk = []
    for r in rows:
        c = r['TargetColumn']; t = r['TargetDataType']
        if c and t and c not in seen:
            col_lines.append(f'  {c} {t}')
            seen.add(c)
        if (str(r.get('IsTargetPK',''))).upper()=='Y' and c not in pk:
            pk.append(c)
    with_items = []
    if 'key' in cfg.columns and 'value' in cfg.columns:
        for _, row in cfg.iterrows():
            k = str(row.get('key','')).strip(); v = str(row.get('value','')).strip()
            if k.lower().startswith('with.') and v:
                with_items.append((k[5:], v))
    if table.upper().startswith('XREF_'):
        has_changelog = any(k == 'changelog.mode' for k, _ in with_items)
        if not has_changelog:
            with_items.append(('changelog.mode', 'upsert'))
    vp = cfg_get(cfg, 'table_value_format', 'avro-registry')
    props = [f"'value.format'='{vp}'"] + [f"'{k}'='{v}'" for k, v in with_items]
    if pk:
        col_lines.append('  ' + f",PRIMARY KEY ({', '.join(pk)}) NOT ENFORCED")
    ddl = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\n' + ',\n'.join(col_lines) + '\n)'
    if props:
        ddl += '\nWITH (\n  ' + ', '.join(props) + '\n)'
    ddl += ';'
    return ddl

def join_clause(rows: List[dict]) -> str:
    for r in rows:
        jt = str(r.get('JoinTable','')).strip()
        jc = str(r.get('JoinCondition','')).strip()
        if jt and jc:
            jty = (str(r.get('JoinType','')) or 'LEFT').upper()
            if jty not in {'INNER','LEFT','RIGHT','FULL'}:
                jty = 'LEFT'
            ja = str(r.get('JoinAlias','')).strip() or 'j'
            return f'\n  {jty} JOIN {jt} {ja} ON {jc}'
    return ''

def insert_sql(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    global cfg_ref; cfg_ref = cfg
    cols = [r['TargetColumn'] for r in rows if r.get('TargetColumn')]
    selects = [f"  {choose_expr(r, False)} AS {r['TargetColumn']}" for r in rows if r.get('TargetColumn')]
    drv = next((f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}" for r in rows if r.get('SourcePrimaryTable','')), '')
    if not drv:
        fb = cfg_get(cfg, 'raw_table_name', cfg_get(cfg, 'default_source_table', '')).strip()
        drv = f'{qident(fb)} t' if fb else '(VALUES(1)) t(dummy)'
    join = join_clause(rows)
    return 'INSERT INTO ' + table + ' (' + ', '.join(cols) + ')\nSELECT\n' + ',\n'.join(selects) + f'\nFROM {drv}{join};'

def classify_target(name: str) -> str:
    u = (name or '').upper()
    if u.endswith('_VIEW') or u.endswith(' VIEW'):
        return 'VIEW'
    if u.startswith('XREF_'):
        return 'XREF'
    if u.startswith('FGAC_'):
        return 'FGAC'
    return 'FGAC'

def generate(sttm_path: Path, out_dir: Path):
    xl = pd.ExcelFile(sttm_path)
    cfg = read_cfg(xl)
    sheet_name = 'STTM_Mapping' if 'STTM_Mapping' in xl.sheet_names else ('STTM' if 'STTM' in xl.sheet_names else xl.sheet_names[0])
    df = norm_cols(pd.read_excel(xl, sheet_name=sheet_name, dtype=str).fillna(''))
    if 'PipelineStage' not in df.columns and 'pipelinestage' in [c.lower() for c in df.columns]:
        for c in df.columns:
            if c.lower()=='pipelinestage':
                df = df.rename(columns={c:'PipelineStage'})
                break
    df['_s'] = df['PipelineStage'].apply(stage_rank)
    df['_p'] = df.get('IsTargetPK', '').apply(lambda x: 0 if str(x).upper()=='Y' else 1)
    df = df.sort_values(by=['_s','TargetTable','_p','TargetColumn'], na_position='last').drop(columns=['_s','_p'], errors='ignore')

    # VALIDATIONS
    basic_warns = validate_views_basic(df)
    deep_issues = validate_alignment(df, lambda c,k,d='': cfg_get(c,k,d))
    all_issues = {"warnings": basic_warns + deep_issues.get("warnings", []), "errors": deep_issues.get("errors", [])}
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_issues_csv(out_dir, all_issues)

    # Continue to SQL generation regardless; caller can fail on error via CLI flag
    grouped: Dict[str, List[dict]] = {}
    for _, r in df.iterrows():
        row = {c: str(r.get(c, '')).strip() for c in df.columns}
        t = row.get('TargetTable','')
        if not t:
            continue
        grouped.setdefault(t, []).append(row)
    views_sql, ddls_sql, xref_sql, fgac_sql = [], [], [], []
    for t, rows in grouped.items():
        kind = classify_target(t)
        t_emitted = apply_prefix_suffix(t, cfg, is_view=(kind=='VIEW'))
        if kind == 'VIEW':
            views_sql.append(f'-- >>> {t_emitted}\n' + build_view_sql(t_emitted, rows, cfg))
        else:
            ddls_sql.append(f'-- >>> {t_emitted}\n' + iceberg_table_ddl(t_emitted, rows, cfg))
            if kind == 'XREF':
                xref_sql.append(f'-- >>> {t_emitted}\n' + insert_sql(t_emitted, rows, cfg))
            else:
                fgac_sql.append(f'-- >>> {t_emitted}\n' + insert_sql(t_emitted, rows, cfg))
    sections = []
    if views_sql:
        sections.append('-- ===== VIEWS =====\n' + '\n\n'.join(views_sql).strip())
    if ddls_sql:
        sections.append('-- ===== TABLES (Kafka + Avro) =====\n' + '\n\n'.join(ddls_sql).strip())
    if xref_sql or fgac_sql:
        parts = []
        if xref_sql:
            parts.append('\n\n'.join(xref_sql).strip())
        if fgac_sql:
            parts.append('\n\n'.join(fgac_sql).strip())
        stmtset = 'EXECUTE STATEMENT SET\nBEGIN\n\n' + ('\n\n'.join(parts)) + '\n\nEND;'
        sections.append('-- ===== INSERT STATEMENT SET =====\n' + stmtset)
    all_sql = ('\n\n'.join(sections)).strip() + '\n'
    (out_dir / '00_all.sql').write_text(all_sql, encoding='utf-8')
    return all_issues, all_sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sttm', required=True, help='Path to STTM workbook (xlsx)')
    ap.add_argument('--out-dir', required=True, help='Output directory for consolidated SQL')
    ap.add_argument('--fail-on-error', action='store_true', help='Exit non-zero if validation errors are found')
    args = ap.parse_args()
    issues, _ = generate(Path(args.sttm), Path(args.out_dir))
    errs = issues.get("errors", [])
    warns = issues.get("warnings", [])
    if errs:
        print("ERRORS:")
        for e in errs:
            print(" -", e)
        print("See issues.csv for full details.")
        if args.fail_on_error:
            raise SystemExit(2)
    if warns:
        print("WARNINGS:")
        for w in warns:
            print(" -", w)
        print("See issues.csv for full details.")
    if not errs and not warns:
        print("[done] OK (no validation issues).")

if __name__ == '__main__':
    main()
