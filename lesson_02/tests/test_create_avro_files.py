import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import os


# Assuming the function to test looks like this
def get_json_files(directory: str, date: str) -> list:
    json_directory = Path("PROJECT_DIRECTORY", "file_storage", directory, date)
    files_list = []

    try:
        with os.scandir(json_directory) as it:
            if not any(it):
                raise OSError("Directory is empty")

            for json_file in os.listdir(json_directory):
                if os.path.isfile(os.path.join(json_directory, json_file)):
                    files_list.append(
                        (
                            os.path.join(json_directory, json_file), json_file
                        )
                    )
    except OSError as e:
        print("Error:", e)

    return files_list


class TestGetJsonFiles(unittest.TestCase):

    @patch('os.scandir')
    @patch('os.listdir')
    @patch('os.path.isfile')
    def test_valid_directory_with_files(
            self,
            mock_isfile,
            mock_listdir,
            mock_scandir
    ):
        # Mocking os.scandir to behave like a context manager with non-empty iterator
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = [MagicMock()]
        mock_scandir.return_value = mock_context_manager

        # Mocking os.listdir to return files
        mock_listdir.return_value = ['file1.json', 'file2.json']

        # Mocking os.path.isfile to return True for both files
        mock_isfile.side_effect = [True, True]  # Both files are valid

        result = get_json_files('test_dir', '2023-09-27')

        expected = [
            (
                str(
                    Path(
                        "PROJECT_DIRECTORY/file_storage/"
                        "test_dir/2023-09-27/file1.json"
                    )
                ), 'file1.json'),
            (
                str(
                    Path(
                        "PROJECT_DIRECTORY/file_storage/"
                        "test_dir/2023-09-27/file2.json"
                    )
                ), 'file2.json')
        ]

        self.assertEqual(result, expected)

    @patch('os.scandir')
    def test_empty_directory(self, mock_scandir):
        # Mocking os.scandir to return a context manager
        # that returns an empty iterator
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = iter([])  # Empty iterator
        mock_scandir.return_value = mock_context_manager

        result = get_json_files('empty_dir', '2023-09-27')
        self.assertEqual(result, [])

    @patch('os.scandir')
    def test_nonexistent_directory(self, mock_scandir):
        # Simulating directory that doesn't exist
        mock_scandir.side_effect = OSError("Directory does not exist")

        result = get_json_files('nonexistent_dir', '2023-09-27')
        self.assertEqual(result, [])

    @patch('os.scandir')
    def test_oserror_in_scandir(self, mock_scandir):
        # Simulate scandir raising an OSError for other reasons
        mock_scandir.side_effect = OSError("General OSError")

        result = get_json_files('error_dir', '2023-09-27')
        self.assertEqual(result, [])
