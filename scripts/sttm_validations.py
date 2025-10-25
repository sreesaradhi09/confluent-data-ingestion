# sttm_validations.py
from typing import Dict, List, Callable
from pathlib import Path
import pandas as pd
import re

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

def validate_views_basic(df: pd.DataFrame) -> List[str]:
    issues = []
    if 'PipelineStage' not in df.columns:
        return issues
    views = df[df['PipelineStage'].str.upper()=='VIEW']
    for i, r in views.iterrows():
        mf = str(r.get('MessageFormat','')).upper()
        key = str(r.get('FieldSelector','')).strip()
        src = str(r.get('SourceField','')).strip()
        if not src and mf=='JSON' and not key:
            issues.append(f'WARN row {i}: JSON View missing key (SourceField or FieldSelector)')
        if mf=='JSON' and (src or key) and (src or key).startswith('$'):
            issues.append(f"WARN row {i}: JSON key must not start with '$'")
    return issues

def validate_alignment(df: pd.DataFrame, cfg_get: Callable) -> Dict[str, list]:
    errors, warns = [], []
    if "TargetTable" not in df.columns:
        errors.append("Missing required column: TargetTable.")
        return {"errors": errors, "warnings": warns}

    missing_tt_rows = [idx for idx, r in df.iterrows() if not str(r.get("TargetTable", "")).strip()]
    if missing_tt_rows:
        for i in missing_tt_rows:
            errors.append(f"[row {i}] TargetTable is required but empty.")

    declared_targets = set(str(x).strip() for x in df["TargetTable"].tolist() if str(x).strip())

    if "TargetColumn" not in df.columns:
        errors.append("Missing required column: TargetColumn.")
        return {"errors": errors, "warnings": warns}

    for tname, tdf in df.groupby("TargetTable"):
        if not str(tname).strip():
            continue
        stage = _u(str(tdf["PipelineStage"].iloc[0] if "PipelineStage" in tdf.columns else "")) or "FGAC"
        rows = [ {c: str(r.get(c, "")).strip() for c in tdf.columns} for _, r in tdf.iterrows() ]

        tgt_cols = [r.get("TargetColumn","") for r in rows if r.get("TargetColumn","")]
        if not tgt_cols:
            errors.append(f"[{tname}] has no TargetColumn entries.")
            continue

        dups = [c for c in tgt_cols if tgt_cols.count(c) > 1]
        if dups:
            for c in _uniq(dups):
                errors.append(f"[{tname}] duplicate TargetColumn: {c}")

        pk_cols = [r["TargetColumn"] for r in rows if _u(r.get("IsTargetPK","")) == "Y" and r.get("TargetColumn")]
        for pk in pk_cols:
            if pk not in tgt_cols:
                errors.append(f"[{tname}] PK column not found among TargetColumns: {pk}")
        if len(pk_cols) != len(set(pk_cols)):
            warns.append(f"[{tname}] duplicate PK marks on same column(s): {', '.join(pk_cols)}")

        spts = [r.get("SourcePrimaryTable","") for r in rows if r.get("SourcePrimaryTable","")]
        spts_uniq = list(_uniq(spts))
        if not spts_uniq:
            errors.append(f"[{tname}] missing SourcePrimaryTable (at least one row must specify it).")
        elif stage == "VIEW" and len(spts_uniq) > 1:
            warns.append(f"[{tname}] VIEW uses multiple SourcePrimaryTable values: {spts_uniq}")

        if stage == "VIEW":
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
                elif mf == "CSV":
                    if not ov and not st and fsel and not _is_int(fsel):
                        errors.append(f"[{tname}] row#{i} CSV FieldSelector must be numeric when provided. Got: {fsel}")

            fp_candidates = [r.get("FilterPredicate","").strip() for r in rows if _u(r.get("IsTargetPK","")) == "Y" and r.get("FilterPredicate","").strip()]
            if fp_candidates:
                raw = fp_candidates[0]
                if re.match(r"^\s*(WHERE|AND|OR)\b", raw, flags=re.IGNORECASE):
                    warns.append(f"[{tname}] FilterPredicate should be the condition only; drop the leading WHERE/AND/OR.")
        else:
            for i, r in enumerate(rows, start=1):
                ov = r.get("ExprOverride","").strip()
                st = r.get("SourceTransformExpr","").strip()
                if ov and st:
                    warns.append(f"[{tname}] row#{i} ExprOverride present; SourceTransformExpr will be ignored.")

            jt_vals = [r.get("JoinTable","").strip() for r in rows if r.get("JoinTable","").strip()]
            jc_vals = [r.get("JoinCondition","").strip() for r in rows if r.get("JoinCondition","").strip()]
            has_jt = bool(jt_vals)
            has_jc = bool(jc_vals)
            if has_jt and not has_jc:
                warns.append(f"[{tname}] JoinTable specified but JoinCondition missing.")
            if has_jc and not has_jt:
                errors.append(f"[{tname}] JoinCondition provided but JoinTable is empty.")
            for jt in jt_vals:
                if not jt:
                    errors.append(f"[{tname}] JoinTable is empty where a join is defined.")
                else:
                    if jt not in declared_targets:
                        warns.append(f"[{tname}] JoinTable '{jt}' is not declared as a TargetTable in STTM (assuming external or pre-existing).")

    return {"errors": errors, "warnings": warns}

def write_issues_csv(out_dir: Path, issues: Dict[str, list]):
    rows = []
    for e in issues.get("errors", []):
        rows.append({"level":"ERROR", "message": e})
    for w in issues.get("warnings", []):
        rows.append({"level":"WARN", "message": w})
    if not rows:
        rows.append({"level":"INFO", "message":"No issues found"})
    pd.DataFrame(rows).to_csv(Path(out_dir) / "issues.csv", index=False)
