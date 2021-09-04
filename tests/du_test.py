import sys
import unittest
from io import StringIO
from srs.bq_du.du import format_size
from srs.bq_du.du import csv_output_formatter
from srs.bq_du.du import raw_output_formatter, R_JUST_SIZE


class FormatSizeTestCase(unittest.TestCase):
    def test_format_raw_bytes(self):
        self.assertEqual(format_size(1000), '1000B')
        self.assertEqual(format_size(2048), '2048B')
        self.assertEqual(format_size(3145728), '3145728B')

    def test_format_human(self):
        self.assertEqual(format_size(3298534883328, 'h'), '3TB')
        self.assertEqual(format_size(3221225472, 'h'),    '3GB')
        self.assertEqual(format_size(3145728, 'h'),       '3MB')
        self.assertEqual(format_size(3072, 'h'),          '3KB')
        self.assertEqual(format_size(30, 'h'),            '30B')


class FormatterTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.stdout = sys.stdout
        sys.stdout = self.string_out = StringIO()

    def test_raw_formatter(self):
        raw_output_formatter([['a', '1', 30]], 'h')
        self.assertEqual(self.string_out.getvalue(), '{}\t{}\n'.format('30B'.rjust(R_JUST_SIZE), 'a'))

    def test_csv_formatter(self):
        csv_output_formatter([['a', '1', 30]], 'h')
        self.assertEqual(self.string_out.getvalue(), 'a,1,30B\n')

    def tearDown(self):
        super().tearDown()
        sys.stdout = self.stdout


if __name__ == '__main__':
    unittest.main()
