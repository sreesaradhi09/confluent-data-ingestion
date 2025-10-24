# sttm_validations.py
# Validation helpers for STTM -> Flink SQL generator (used by sttm_to_flink_v21.py)

from typing import Dict, List
import pandas as pd
import re
from pathlib import Path

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
    """Basic sanity checks for Views (legacy quick validator retained)."""
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

def validate_alignment(df: pd.DataFrame, cfg_get) -> Dict[str, list]:
    """Schema-free but comprehensive workbook consistency validation."""
    errors, warns = [], []

    if "TargetTable" not in df.columns or "TargetColumn" not in df.columns:
        errors.append("Missing required columns TargetTable / TargetColumn.")
        return {"errors": errors, "warnings": warns}

    # Group by target table
    for tname, tdf in df.groupby("TargetTable"):
        stage = _u(str(tdf["PipelineStage"].iloc[0] if "PipelineStage" in tdf.columns else "")) or "FGAC"
        rows = [ {c: str(r.get(c, "")).strip() for c in tdf.columns} for _, r in tdf.iterrows() ]

        # 1) Target columns presence
        tgt_cols = [r.get("TargetColumn","") for r in rows if r.get("TargetColumn","")]
        if not tgt_cols:
            errors.append(f"[{tname}] has no TargetColumn entries.")
            continue

        # 2) Duplicate target columns in a table
        dups = [c for c in tgt_cols if tgt_cols.count(c) > 1]
        if dups:
            for c in _uniq(dups):
                errors.append(f"[{tname}] duplicate TargetColumn: {c}")

        # 3) PK sanity
        pk_cols = [r["TargetColumn"] for r in rows if _u(r.get("IsTargetPK","")) == "Y" and r.get("TargetColumn")]
        for pk in pk_cols:
            if pk not in tgt_cols:
                errors.append(f"[{tname}] PK column not found among TargetColumns: {pk}")
        if len(pk_cols) != len(set(pk_cols)):
            warns.append(f"[{tname}] duplicate PK marks on same column(s): {', '.join(pk_cols)}")

        # 4) Source primary table consistency (nice-to-have)
        spts = list(_uniq([r.get("SourcePrimaryTable","") for r in rows if r.get("SourcePrimaryTable","")]))
        if stage == "VIEW" and len(spts) > 1:
            warns.append(f"[{tname}] VIEW uses multiple SourcePrimaryTable values: {spts}")

        # Stage-specific checks
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
                    # FieldSelector numeric only when provided (auto-assigned otherwise)
                    if not ov and not st and fsel and not _is_int(fsel):
                        errors.append(f"[{tname}] row#{i} CSV FieldSelector must be numeric when provided. Got: {fsel}")

            # FilterPredicate sanity (just check analysts didn't include WHERE)
            fp_candidates = [r.get("FilterPredicate","").strip() for r in rows if _u(r.get("IsTargetPK","")) == "Y" and r.get("FilterPredicate","").strip()]
            if fp_candidates:
                raw = fp_candidates[0]
                if re.match(r"^\s*(WHERE|AND|OR)\b", raw, flags=re.IGNORECASE):
                    warns.append(f"[{tname}] FilterPredicate should be the condition only; drop the leading WHERE/AND/OR.")

        else:
            # XREF / FGAC: precedence sanity
            for i, r in enumerate(rows, start=1):
                ov = r.get("ExprOverride","").strip()
                st = r.get("SourceTransformExpr","").strip()
                if ov and st:
                    warns.append(f"[{tname}] row#{i} ExprOverride present; SourceTransformExpr will be ignored.")

            # Join completeness
            jt = any([r.get("JoinTable","").strip() for r in rows])
            jc = any([r.get("JoinCondition","").strip() for r in rows])
            if jt and not jc:
                warns.append(f"[{tname}] JoinTable specified but JoinCondition missing.")
            if jc and not jt:
                warns.append(f"[{tname}] JoinCondition specified but JoinTable missing.")

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
