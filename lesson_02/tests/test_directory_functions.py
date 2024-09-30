import unittest
from unittest.mock import patch
from pathlib import Path
import os

from lesson_02.settings.constants import PROJECT_DIRECTORY
from lesson_02.functions.directory_functions import (remove_all_files_from_directory,
                                                     create_or_clean_is_exists_directory)


class TestRemoveFiles(unittest.TestCase):

    @patch('os.listdir')
    @patch('os.path.isfile')
    @patch('os.path.islink')
    @patch('os.unlink')
    @patch('os.path.isdir')
    @patch('shutil.rmtree')
    def test_remove_all_files(
            self,
            mock_rmtree,
            mock_isdir,
            mock_unlink,
            mock_islink,
            mock_isfile,
            mock_listdir
    ):
        # Arrange
        directory = Path("/mock_directory")
        mock_listdir.return_value = ['file1.txt', 'file2.txt', 'subdir']

        # file1.txt is a regular file
        mock_isfile.side_effect = lambda x: True if 'file1.txt' in x else False
        # file2.txt is a symlink
        mock_islink.side_effect = lambda x: True if 'file2.txt' in x else False
        # subdir is a directory
        mock_isdir.side_effect = lambda x: True if 'subdir' in x else False

        # Act
        remove_all_files_from_directory(directory)

        # Assert
        mock_listdir.assert_called_once_with(directory)
        mock_unlink.assert_any_call(os.path.join(directory, 'file1.txt'))
        mock_unlink.assert_any_call(os.path.join(directory, 'file2.txt'))
        mock_rmtree.assert_called_once_with(os.path.join(directory, 'subdir'))

    @patch("os.makedirs")
    @patch("os.path.exists")
    @patch("os.chdir")
    @patch("lesson_02.directory_functions.remove_all_files_from_directory")
    def test_create_or_clean_is_exists_directory(
            self,
            mock_remove_all_files,
            mock_chdir,
            mock_exists,
            mock_makedirs
    ):
        # Test when the directory exists
        mock_exists.return_value = True

        path = "test_subdirectory"
        date = "2024-09-28"
        storage_directory = Path(PROJECT_DIRECTORY, "file_storage", path, date)

        create_or_clean_is_exists_directory(True, path, date)

        # Ensure remove_all_files_from_directory is called when directory exists
        mock_remove_all_files.assert_called_once_with(storage_directory)

        # Ensure os.chdir is called with the correct path
        mock_chdir.assert_called_once_with(storage_directory)

        # Test when the directory does not exist
        mock_exists.return_value = False

        create_or_clean_is_exists_directory(True, path, date)

        # Ensure os.makedirs is called when directory doesn't exist
        mock_makedirs.assert_called_once_with(storage_directory)

        # Ensure os.chdir is called again with the correct path
        mock_chdir.assert_called_with(storage_directory)
