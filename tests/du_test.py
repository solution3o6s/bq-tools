import sys
import random
import unittest
import unittest.mock as mock

from io import StringIO
from bq_du.du import raw_size
from bq_du.du import travel_fields
from bq_du.du import human_readable_size
from bq_du.du import csv_output_formatter
from bq_du.du import raw_output_formatter, HUMAN_PADDING


def mock_du_field(_):
    return random.randint(10, 3072)


class FormatSizeTestCase(unittest.TestCase):
    def test_format_raw_bytes(self):
        self.assertEqual(raw_size(1000), '1000B')
        self.assertEqual(raw_size(2048), '2048B')
        self.assertEqual(raw_size(3145728), '3145728B')

    def test_format_human(self):
        self.assertEqual(human_readable_size(3298534883328), '3TB')
        self.assertEqual(human_readable_size(3221225472),    '3GB')
        self.assertEqual(human_readable_size(3145728),       '3MB')
        self.assertEqual(human_readable_size(3072),          '3KB')
        self.assertEqual(human_readable_size(30),            '30B')


class FormatterTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.stdout = sys.stdout
        sys.stdout = self.string_out = StringIO()

    def test_raw_formatter(self):
        raw_output_formatter([['a', '1', 30]], ['h'])
        self.assertEqual(self.string_out.getvalue(), '{}\t{}\n'.format('30B'.rjust(HUMAN_PADDING), 'a'))

    def test_csv_formatter(self):
        csv_output_formatter([['a', '1', 30]], ['h'])
        self.assertEqual(self.string_out.getvalue(), 'field,level,size\na,1,30B\n')

    def tearDown(self):
        super().tearDown()
        sys.stdout = self.stdout


class TravelFieldsTestCase(unittest.TestCase):
    def setUp(self):
        self.fields = [
            {
              "mode": "NULLABLE",
              "name": "field_a_0",
              "type": "TIMESTAMP"
            },
            {
              "mode": "NULLABLE",
              "name": "field_b_0",
              "type": "STRING"
            },
            {
              "fields": [
                {
                  "mode": "NULLABLE",
                  "name": "field_a_1",
                  "type": "TIMESTAMP"
                },
                {
                  "fields": [
                    {
                      "mode": "NULLABLE",
                      "name": "field_a_2",
                      "type": "STRING"
                    },
                    {
                      "mode": "NULLABLE",
                      "name": "field_b_2",
                      "type": "STRING"
                    }
                  ],
                  "mode": "NULLABLE",
                  "name": "record_b_1",
                  "type": "RECORD"
                },
                {
                  "mode": "NULLABLE",
                  "name": "field_c_1",
                  "type": "STRING"
                }
              ],
              "mode": "NULLABLE",
              "name": "record_c_0",
              "type": "RECORD"
            }
        ]

    @mock.patch('bq_du.du.du_field', mock_du_field)
    def assert_travel_by_depth(self, expected_fields_count, expected_fields_depth, travel_depth):
        actual_fields_count = 0
        actual_fields_depth = 0

        for du_data in travel_fields(self.fields, travel_depth):
            actual_fields_count += 1
            actual_fields_depth = max(actual_fields_depth, int(du_data[0][-1]))

        self.assertEqual(actual_fields_count, expected_fields_count, 'Failed to match fields count.')
        self.assertEqual(actual_fields_depth, expected_fields_depth, 'Failed to match fields depth.')

    def test_travel_depth_all(self):
        expected_fields_count = 8
        expected_fields_depth = 2
        self.assert_travel_by_depth(expected_fields_count, expected_fields_depth, 3)
        self.assert_travel_by_depth(expected_fields_count, expected_fields_depth, -1)

    def test_travel_depth_1(self):
        expected_fields_count = 3
        expected_fields_depth = 0
        self.assert_travel_by_depth(expected_fields_count, expected_fields_depth, 1)

    def test_travel_depth_2(self):
        expected_fields_count = 6
        expected_fields_depth = 1
        self.assert_travel_by_depth(expected_fields_count, expected_fields_depth, 2)


if __name__ == '__main__':
    unittest.main()
