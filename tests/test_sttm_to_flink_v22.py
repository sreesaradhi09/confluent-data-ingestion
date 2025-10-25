import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from sttm_to_flink_v22 import (  # type: ignore  # pylint: disable=import-error
    norm_cols,
    qident,
    sanitize_predicate,
    rewrite_predicate_as_json,
    choose_expr,
    resolve_table_props,
    build_view_sql,
    build_table_ddl,
    build_insert_sql,
    generate,
)


class HelperFunctionTests(unittest.TestCase):
    def test_norm_cols_strips_whitespace_and_nan(self):
        df = pd.DataFrame(
            {"  Col A ": ["  value  ", None, " NaN "]}
        )
        result = norm_cols(df)
        self.assertEqual(list(result.columns), ["Col A"])
        self.assertEqual(result.iloc[0, 0], "value")
        self.assertEqual(result.iloc[1, 0], "None")
        self.assertEqual(result.iloc[2, 0], "")

    def test_qident_wraps_plain_identifiers(self):
        self.assertEqual(qident("table"), "`table`")
        self.assertEqual(qident("  already "), "`already`")
        self.assertEqual(qident("`quoted`"), "`quoted`")
        self.assertEqual(qident(" (select) "), "(select)")

    def test_sanitize_predicate_removes_leading_keywords(self):
        self.assertEqual(sanitize_predicate("WHERE status = 'A'"), "status = 'A'")
        self.assertEqual(sanitize_predicate("  AND id = 5 ;;;"), "id = 5 ")
        self.assertEqual(sanitize_predicate(""), "")

    def test_rewrite_predicate_as_json_rewrites_tokens_outside_quotes(self):
        raw = "STATUS = 'ACTIVE' AND EVENT_ID = 10"
        rewritten = rewrite_predicate_as_json(raw, "payload")
        self.assertIn("JSON_VALUE(CAST(payload AS STRING), '$.STATUS')", rewritten)
        self.assertIn("JSON_VALUE(CAST(payload AS STRING), '$.EVENT_ID')", rewritten)
        self.assertIn("'ACTIVE'", rewritten)


class ExpressionBuilderTests(unittest.TestCase):
    def test_choose_expr_view_prefers_override_without_double_cast(self):
        row = {
            "ExprOverride": "CAST(val AS INT)",
            "SourceTransformExpr": "",
            "TargetDataType": "INT",
            "MessageFormat": "JSON",
            "SourceField": "id",
            "FieldSelector": "",
            "TargetColumn": "id",
        }
        expr = choose_expr(row, True, "payload", ",", {})
        self.assertEqual(expr, "CAST(val AS INT)")

    def test_choose_expr_view_casts_source_transform(self):
        row = {
            "ExprOverride": "",
            "SourceTransformExpr": "payload->>'id'",
            "TargetDataType": "BIGINT",
            "MessageFormat": "JSON",
            "SourceField": "id",
            "FieldSelector": "",
            "TargetColumn": "id",
        }
        expr = choose_expr(row, True, "payload", ",", {})
        self.assertEqual(expr, "CAST(payload->>'id' AS BIGINT)")

    def test_choose_expr_json_fallback_uses_target_column(self):
        row = {
            "ExprOverride": "",
            "SourceTransformExpr": "",
            "TargetDataType": "STRING",
            "MessageFormat": "JSON",
            "SourceField": "",
            "FieldSelector": "",
            "TargetColumn": "target_col",
        }
        expr = choose_expr(row, True, "payload_col", ",", {})
        self.assertIn("JSON_VALUE(CAST(payload_col AS STRING), '$.target_col')", expr)
        self.assertTrue(expr.startswith("CAST("))

    def test_choose_expr_csv_uses_auto_index(self):
        row = {
            "ExprOverride": "",
            "SourceTransformExpr": "",
            "TargetDataType": "STRING",
            "MessageFormat": "CSV",
            "SourceField": "",
            "FieldSelector": "",
            "TargetColumn": "col2",
        }
        expr = choose_expr(row, True, "payload_col", "|", {"col2": 3})
        self.assertIn("SPLIT_INDEX(CAST(payload_col AS STRING), '|', 3)", expr)

    def test_choose_expr_non_view_prefers_source_field(self):
        row = {
            "ExprOverride": "",
            "SourceTransformExpr": "",
            "TargetDataType": "STRING",
            "SourceField": "source_col",
            "TargetColumn": "target_col",
        }
        expr = choose_expr(row, False, "payload", ",", {})
        self.assertEqual(expr, "source_col")


class ResolvePropsTests(unittest.TestCase):
    def test_resolve_table_props_handles_blank_header(self):
        matrix_df = pd.DataFrame(
            [
                ["format", "${table_name}", "", "ignored"],
                ["connector", "jdbc", "", "ignored"],
            ],
            columns=["Key", "  MyTable ", "", "OtherTable"],
        )

        props = resolve_table_props("MyTable", "MyTable", matrix_df)

        self.assertEqual(
            props,
            {
                "format": "MyTable",
                "connector": "jdbc",
            },
        )


class SQLBuilderTests(unittest.TestCase):
    def test_build_view_sql_backticks_table_name(self):
        rows = [
            {
                "__expr__": "expr1",
                "TargetColumn": "col1",
                "SourcePrimaryTable": "source_table",
                "SourcePrimaryAlias": "s",
            }
        ]
        sql = build_view_sql("view_table", rows, "payload", "")
        self.assertTrue(sql.startswith("CREATE VIEW `view_table`"))

    def test_build_table_ddl_includes_primary_key(self):
        rows = [
            {"TargetColumn": "id", "TargetDataType": "INT", "IsTargetPK": "Y"},
            {"TargetColumn": "name", "TargetDataType": "STRING", "IsTargetPK": ""},
        ]
        ddl = build_table_ddl("main_table", rows, {"connector": "jdbc"})
        self.assertIn("CREATE TABLE IF NOT EXISTS `main_table`", ddl)
        self.assertIn("PRIMARY KEY (id) NOT ENFORCED", ddl)
        self.assertIn("'connector' = 'jdbc'", ddl)

    def test_build_insert_sql_quotes_all_table_names(self):
        rows = [
            {
                "TargetColumn": "col1",
                "__expr__": "expr1",
                "SourcePrimaryTable": "base_table",
                "SourcePrimaryAlias": "t",
                "JoinTable": "lookup_table",
                "JoinCondition": "j.id = t.id",
                "JoinType": "LEFT",
                "JoinAlias": "j",
            }
        ]

        sql = build_insert_sql("main_table", rows, "")

        self.assertIn("INSERT INTO `main_table`", sql)
        self.assertIn("FROM `base_table` t", sql)
        self.assertIn("JOIN `lookup_table` j ON", sql)


class GenerateTests(unittest.TestCase):
    def test_generate_creates_expected_sql_sections(self):
        mapping_df = pd.DataFrame(
            [
                {
                    "TargetTable": "view_table",
                    "TargetColumn": "id",
                    "PipelineStage": "VIEW",
                    "TargetDataType": "STRING",
                    "MessageFormat": "JSON",
                    "SourceField": "id",
                    "FieldSelector": "",
                    "SourcePrimaryTable": "source_table",
                    "SourcePrimaryAlias": "s",
                    "IsTargetPK": "Y",
                    "FilterPredicate": "status = 'ACTIVE'",
                },
                {
                    "TargetTable": "view_table",
                    "TargetColumn": "name",
                    "PipelineStage": "VIEW",
                    "TargetDataType": "STRING",
                    "MessageFormat": "CSV",
                    "FieldSelector": "",
                    "SourceField": "",
                    "SourcePrimaryTable": "source_table",
                    "SourcePrimaryAlias": "s",
                    "IsTargetPK": "",
                },
                {
                    "TargetTable": "main_table",
                    "TargetColumn": "id",
                    "PipelineStage": "FGAC",
                    "TargetDataType": "INT",
                    "SourceField": "s.id",
                    "SourcePrimaryTable": "src_table",
                    "SourcePrimaryAlias": "s",
                    "IsTargetPK": "Y",
                    "JoinTable": "lookup_table",
                    "JoinCondition": "j.id = s.id",
                    "JoinType": "LEFT",
                    "JoinAlias": "j",
                    "FilterPredicate": "s.is_active = TRUE",
                },
                {
                    "TargetTable": "main_table",
                    "TargetColumn": "name",
                    "PipelineStage": "FGAC",
                    "TargetDataType": "STRING",
                    "SourceField": "s.name",
                    "SourcePrimaryTable": "src_table",
                    "SourcePrimaryAlias": "s",
                    "IsTargetPK": "",
                },
            ]
        )

        matrix_df = pd.DataFrame(
            {
                "Key": ["connector", "format"],
                "view_table": ["jdbc", "${table_name}"],
                "main_table": ["kafka", "json"],
            }
        )

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            workbook_path = tmp_path / "sttm.xlsx"
            out_dir = tmp_path / "out"
            with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
                mapping_df.to_excel(writer, sheet_name="STTM_Mapping", index=False)
                matrix_df.to_excel(writer, sheet_name="Config_TableMatrix", index=False)

            issues, sql = generate(workbook_path, out_dir)

            self.assertFalse(issues["errors"])
            self.assertTrue(out_dir.exists())
            output_file = out_dir / "00_all.sql"
            self.assertTrue(output_file.exists())

            generated_sql = output_file.read_text()
            self.assertIn("-- ===== VIEWS =====", generated_sql)
            self.assertIn("CREATE VIEW `view_table`", generated_sql)
            self.assertIn("CREATE TABLE IF NOT EXISTS `main_table`", generated_sql)
            self.assertIn("INSERT INTO `main_table`", generated_sql)
            self.assertIn("EXECUTE STATEMENT SET", generated_sql)

            self.assertEqual(sql, generated_sql)


if __name__ == "__main__":
    unittest.main()
