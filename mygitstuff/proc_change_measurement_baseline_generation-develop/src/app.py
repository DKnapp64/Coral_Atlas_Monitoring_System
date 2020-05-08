import logging
import os
import tempfile

import click
from proc_gcs_utils import gcs

from src import baseline


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')


def process_folders(gcp_project_name,
                    gcs_bucket_name,
                    source_path,
                    destination_path):

    folders = gcs.list_bucket_folders(gcp_project_name,
                                      gcs_bucket_name,
                                      source_path)

    if not folders:
        raise ValueError('No subfolders found in GCS source folder {0}/{1}'.format(gcs_bucket_name,
                                                                                   source_path))
    for folder in folders:
        temp_dir = tempfile.mkdtemp()
        logging.info('Downloading quads from {0}/{1}'.format(source_path, folder))
        gcs.download_files_from_gcs(gcp_project_name,
                                    gcs_bucket_name,
                                    '{0}/{1}'.format(source_path, folder),
                                    temp_dir)
        if not os.listdir(temp_dir):
            raise ValueError('No files downloaded from {0}/{1}/{2}'.format(gcs_bucket_name,
                                                                           source_path,
                                                                           folder))

        logging.info('Generating baseline from {0}/{1}'.format(source_path, folder))
        baseline_file_name = '{}_baseline.tif'.format(folder)
        baseline.generate_baseline(temp_dir,
                                   os.path.join(temp_dir, baseline_file_name))

        logging.info('Uploading baseline to {0}/{1}'.format(destination_path, baseline_file_name))
        gcs.upload_file_to_gcs(gcp_project_name,
                               gcs_bucket_name,
                               destination_path,
                               os.path.join(temp_dir, baseline_file_name))


@click.command()
@click.argument('gcp_project_name')
@click.argument('gcs_bucket_name')
@click.argument('source_path')
@click.argument('destination_path')
def main(gcp_project_name,
         gcs_bucket_name,
         source_path,
         destination_path):

    process_folders(gcp_project_name,
                    gcs_bucket_name,
                    source_path,
                    destination_path)


if __name__ == '__main__':
    main()
