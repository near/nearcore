import unittest
import datetime
import io
import sys
from send_validator_logs import filter_log_file


class test_validator_log_filtering(unittest.TestCase):

    def test_time_filtering(self):
        start_time = datetime.datetime(2022, 4, 4, 23, 42, 0, 0)
        end_time = datetime.datetime(2022, 4, 4, 23, 49, 0, 0)
        output_file_obj = filter_log_file("./tests/data/node0.logs",
                                          start_time=start_time,
                                          end_time=end_time)
        self.assertIsInstance(
            output_file_obj, list,
            "Parsed file object should be of type io.BytesIO")
        self.assertEqual(len(output_file_obj), 48,
                         "Filtered log should have 48 lines")


if __name__ == '__main__':
    unittest.main()
