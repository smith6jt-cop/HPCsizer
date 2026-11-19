"""Tests for lib/fmt.py: text that comes from jobs must be shown, never acted on."""

import csv
import io

from lib import fmt
from lib.report import to_csv


class TestClean:
    def test_ordinary_text_is_untouched(self):
        for text in ("mplx45k-spleen", "sbatch --wrap='python a.py'", "żółć", "名前", ""):
            assert fmt.clean(text) == text

    def test_escape_sequences_and_line_breaks_become_visible(self):
        assert fmt.clean("a\x1b[31mb") == "a\\x1b[31mb"
        assert fmt.clean("one\ntwo\rthree\tfour") == "one\\x0atwo\\x0dthree\\x09four"
        assert fmt.clean("\x00\x07\x7f\x9b") == "\\x00\\x07\\x7f\\x9b"

    def test_characters_that_reorder_or_hide_text(self):
        assert fmt.clean("safe\u202egnp.exe") == "safe\\u202egnp.exe"
        assert fmt.clean("a\u200bb\u2028c\ufeff") == "a\\u200bb\\u2028c\\ufeff"

    def test_unpaired_surrogate_cannot_break_the_output_encoding(self):
        fmt.clean("x\udc80y").encode("utf-8")

    def test_other_types(self):
        assert fmt.clean(12) == "12" and fmt.clean(None) == "None"


class TestTables:
    def test_a_cell_cannot_start_a_new_line_or_shift_columns(self):
        lines = fmt.table(["Name", "Jobs"], [["x\nrm -rf ~", 3], ["y\tz", 4]]).splitlines()
        assert len(lines) == 3, "a header and two rows"
        assert lines[1] == "x\\x0arm -rf ~  3" and lines[2].split() == ["y\\x09z", "4"]
        assert lines[0].index("Jobs") == lines[1].index("3") == lines[2].index("4")

    def test_markdown_cell_cannot_end_its_cell_or_become_a_link_a_tag_or_code(self):
        text = fmt.markdown_table(["Family"], [["a|b [x](http://e) <img> `c`\nd"]])
        assert text.splitlines()[2] == "| a\\|b \\[x](http://e) \\<img> \\`c\\`\\\\x0ad |"

    def test_markdown_keeps_ordinary_names_as_they_are(self):
        text = fmt.markdown_table(["Family"], [["alice/batch/run_mplx#k-spleen"], [None]])
        assert text.splitlines()[2:] == ["| alice/batch/run_mplx#k-spleen |", "|  |"]


class TestCsv:
    def test_text_that_a_spreadsheet_would_evaluate_is_made_text(self):
        for text in ("=1+1", "+cmd|' /C calc'!A0", "@SUM(A1)", "-2+3", '=HYPERLINK("http://e")'):
            assert fmt.csv_text(text) == "'" + text

    def test_numbers_and_dashes_stay(self):
        for text in ("-", "-77%", "-12.5", "+3", "1e5", "2026-11-19", "alice/batch/x", ""):
            assert fmt.csv_text(text) == text

    def test_one_physical_line_per_row_and_numbers_stay_numbers(self):
        text = to_csv(["name", "n", "x"], [["a\nb", -5, 1.5], ["=evil()", 2, None]])
        assert text.splitlines() == ["name,n,x", "a\\x0ab,-5,1.5", "'=evil(),2,"]
        rows = list(csv.reader(io.StringIO(text)))
        assert rows[1] == ["a\\x0ab", "-5", "1.5"]
