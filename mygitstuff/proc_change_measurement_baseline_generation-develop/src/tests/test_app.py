import os
from unittest.mock import call, MagicMock, patch

from click.testing import CliRunner
import pytest

from src import app


fake_gcp_project_name = 'bogus-project'
fake_gcs_bucket_name = 'bogus-bucket'
fake_source_path = 'path/to/inputs'
fake_destination_path = 'path/to/outputs'
fake_subfolders = ['fake_subfolder_0', 'fake_subfolder_1']
fake_temp_dirs = ['/path/to/fake_temp_dir_0', '/path/to/fake_temp_dir_1']


def test_main():
    mock_process_folders = MagicMock()
    with patch('src.app.process_folders', mock_process_folders):
        runner = CliRunner()
        result = runner.invoke(app.main, [
            fake_gcp_project_name,
            fake_gcs_bucket_name,
            fake_source_path,
            fake_destination_path
        ])

    assert result.exit_code == 0, result.output
    mock_process_folders.assert_called_once_with(fake_gcp_project_name,
                                          fake_gcs_bucket_name,
                                          fake_source_path,
                                          fake_destination_path)


class TestBaseline:

    def test_happy_path(self):
        mock_baseline = MagicMock()
        mock_gcs = MagicMock()
        mock_gcs.list_bucket_folders.return_value = fake_subfolders
        mock_os = MagicMock()
        mock_os.listdir.side_effect = [['foo.tif'], ['bar.tif']]
        mock_os.path.join = os.path.join
        mock_tempfile = MagicMock()
        mock_tempfile.mkdtemp.side_effect = fake_temp_dirs
        with patch.multiple('src.app',
                            baseline=mock_baseline,
                            gcs=mock_gcs,
                            os=mock_os,
                            tempfile=mock_tempfile):
            app.process_folders(fake_gcp_project_name,
                                fake_gcs_bucket_name,
                                fake_source_path,
                                fake_destination_path)

        mock_gcs.list_bucket_folders.assert_called_once_with(fake_gcp_project_name,
                                                             fake_gcs_bucket_name,
                                                             fake_source_path)
        mock_gcs.download_files_from_gcs.assert_has_calls([
            call(fake_gcp_project_name,
                 fake_gcs_bucket_name,
                 '{0}/{1}'.format(fake_source_path, fake_subfolders[0]),
                 fake_temp_dirs[0]),
            call(fake_gcp_project_name,
                 fake_gcs_bucket_name,
                 '{0}/{1}'.format(fake_source_path, fake_subfolders[1]),
                 fake_temp_dirs[1])
        ])
        mock_baseline.generate_baseline.assert_has_calls([
            call(fake_temp_dirs[0],
                 os.path.join(fake_temp_dirs[0], '{0}_baseline.tif'.format(fake_subfolders[0]))),
            call(fake_temp_dirs[1],
                 os.path.join(fake_temp_dirs[1], '{0}_baseline.tif'.format(fake_subfolders[1])))
        ])
        mock_gcs.upload_file_to_gcs.assert_has_calls([
            call(fake_gcp_project_name,
                 fake_gcs_bucket_name,
                 fake_destination_path,
                 os.path.join(fake_temp_dirs[0], '{0}_baseline.tif'.format(fake_subfolders[0]))),
        ])

    def test_raises_if_source_folder_is_empty(self):
        mock_gcs = MagicMock()
        mock_gcs.list_bucket_folders.return_value = []
        with patch('src.app.gcs', mock_gcs):
            with pytest.raises(ValueError):
                app.process_folders(fake_gcp_project_name,
                                    fake_gcs_bucket_name,
                                    fake_source_path,
                                    fake_destination_path)

    def test_raises_if_subfolder_is_empty(self):
        mock_gcs = MagicMock()
        mock_gcs.list_bucket_folders.return_value = fake_subfolders
        mock_os = MagicMock()
        mock_os.listdir.return_value = []
        with patch.multiple('src.app',
                            gcs=mock_gcs,
                            os=mock_os):
            with pytest.raises(ValueError):
                app.process_folders(fake_gcp_project_name,
                                    fake_gcs_bucket_name,
                                    fake_source_path,
                                    fake_destination_path)
