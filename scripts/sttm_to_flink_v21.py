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
            # SourceField holds the JSON key; we emit '$.' + key
            key = sfld or fsel
            base = f"JSON_VALUE(CAST({cfg_payload_default()} AS STRING), '$.{key}')"
        elif mf == 'CSV':
            # delimiter from Config (csv_delimiter), default ','
            delim = ','
            try:
                if 'cfg_ref' in globals() and 'key' in cfg_ref.columns and 'value' in cfg_ref.columns:
                    sel = cfg_ref[cfg_ref['key']=='csv_delimiter']
                    if not sel.empty:
                        delim = str(sel['value'].iloc[0] or ',')
            except Exception:
                delim = ','
            # ALWAYS use SourceField as payload if provided, else default from Config
            srcp = sfld if sfld else cfg_payload_default()
            # explicit numeric FieldSelector? use it; else use precomputed CSV_AUTO_INDEX
            if re.match(r'^\d+$', fsel or ''):
                idx = fsel
            else:
                idx = CSV_AUTO_INDEX.get(row.get('TargetColumn'), '0') if 'CSV_AUTO_INDEX' in globals() else '0'
            base = f"SPLIT_INDEX(CAST({srcp} AS STRING), '{delim}', {idx})"
        else:
            base = sfld or cfg_payload_default()

        norm = f'TRIM({base})' if tgt.upper().startswith('STRING') else f"NULLIF(TRIM({base}), '')"
        return f'CAST({norm} AS {tgt})'

    # Non-views: no auto JSON/CSV or casts unless provided
    if override:
        return override
    if stx:
        return stx
    sf = (row.get('SourceField') or '').strip()
    if sf:
        return sf
    return (row.get('TargetColumn') or '').strip() or 'NULL'

def build_view_sql(table: str, rows: List[dict], cfg: pd.DataFrame) -> str:
    global cfg_ref; cfg_ref = cfg
    # CSV auto-index allocator (mixed scenarios):
    # 1) Reserve explicit numeric selectors
    # 2) Assign auto columns the smallest non-negative unused index in row order
    global CSV_AUTO_INDEX
    CSV_AUTO_INDEX = {}
    reserved = set()

    # pass 1: reserve explicit numeric selectors (only if no override/transform)
    for _r in rows:
        if (_r.get('MessageFormat') or '').upper() != 'CSV':
            continue
        if (_r.get('ExprOverride') or '').strip() or (_r.get('SourceTransformExpr') or '').strip():
            continue  # expressions do not consume an index
        fsel = (_r.get('FieldSelector') or '').strip()
        if re.match(r'^\d+$', fsel or ''):
            reserved.add(int(fsel))

    # helper to get next free index >= start
    def next_free(start: int) -> int:
        i = start
        while i in reserved:
            i += 1
        return i

    # pass 2: assign auto and advance cursor when explicit is present
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
    # Filter predicate only from the first PK row (if present)
    pk_filter = ""
    for r in rows:
        if (str(r.get("IsTargetPK","")).upper()=="Y") and str(r.get("FilterPredicate","")).strip():
            pk_filter = str(r["FilterPredicate"]).strip()
            break
    where = f"\nWHERE {pk_filter}" if pk_filter else ""

    src = next(
        (f"{qident(r.get('SourcePrimaryTable',''))} {r.get('SourcePrimaryAlias','') or 't'}"
         for r in rows if r.get("SourcePrimaryTable","")),
        ""
    )
    if not src:
        fallback = cfg_get(cfg, "raw_table_name", cfg_get(cfg, "default_source_table", "")).strip()
        if fallback:
            src = f"{qident(fallback)} t"
        else:
            src = "(VALUES(1)) t(dummy)"

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

def validate_views(df: pd.DataFrame) -> List[str]:
    issues = []
    views = df[df['PipelineStage'].str.upper()=='VIEW']
    for i, r in views.iterrows():
        override = str(r.get('ExprOverride','')).strip()
        mf = str(r.get('MessageFormat','')).upper()
        key = str(r.get('FieldSelector','')).strip()
        src = str(r.get('SourceField','')).strip()
        if not src and mf=='JSON' and not key:
            issues.append(f'WARN row {i}: JSON View missing key (SourceField or FieldSelector)')
        if mf=='JSON' and (src or key) and (src or key).startswith('$'):
            issues.append(f"WARN row {i}: JSON key must not start with '$'")
    return issues

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
    issues = validate_views(df)
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
    out_dir.mkdir(parents=True, exist_ok=True)
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
    return issues, all_sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sttm', required=True, help='Path to STTM workbook (xlsx)')
    ap.add_argument('--out-dir', required=True, help='Output directory for consolidated SQL')
    args = ap.parse_args()
    issues, _ = generate(Path(args.sttm), Path(args.out_dir))
    if issues:
        print('\n'.join(issues))
        print('[done] with WARNINGS')
    else:
        print('[done] OK')

if __name__ == '__main__':
    main()