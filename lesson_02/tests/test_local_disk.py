import unittest
from unittest.mock import patch, mock_open

from lesson_02.job1.dal.local_disk import save_to_disk


class TestSaveToDisk(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    @patch('os.path.exists', return_value=False)
    @patch('os.chdir')
    def test_save_records_creates_files(
            self,
            mock_chdir,
            mock_exists,
            mock_makedirs,
            mock_open
    ):
        # Sample data
        date = '2024-09-28'
        json_content = [
            {'item': 'item1', 'amount': 10}, {'item': 'item2', 'amount': 20}
        ]
        path = 'test_path'

        # Call the function
        result = save_to_disk(date, json_content, path)

        # Assert the result message
        self.assertEqual(
            result, f"All records for {date} have been added to local storage."
        )

        # Assert that the directory creation was called
        mock_makedirs.assert_called_once()

        # Check if open was called the correct number
        # of times with the correct file names
        self.assertEqual(mock_open.call_count, len(json_content))
        mock_open.assert_any_call(f"sales_{date}_1.json", "w")
        mock_open.assert_any_call(f"sales_{date}_2.json", "w")
