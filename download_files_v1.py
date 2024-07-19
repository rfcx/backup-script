import sys
import os
import csv
import requests
from urllib.parse import urlparse
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools
from datetime import datetime


TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'


def fetch_file(parent_dir, id_column, processed_ids, row):
    """ Downloads file from a given URL
    :param parent_dir: - default target directory
    :param row: - row from csv file, containing url
    :return:
    """
    destination_dir = parent_dir
    item_id = row[id_column]
    url = row['url']

    if item_id in processed_ids:
        return None

    try:
        # If row contains site data - create a subdirectory for the site
        if 'site_id' in row:
            site_id = row['site_id']
            destination_dir = os.path.join(parent_dir, site_id)

        if url:
            file_path = Path(urlparse(url).path)
            filename = file_path.name

            # Generate filename for recordings
            if id_column == 'recording_id':
                date = datetime.strptime(row['datetime'], '%m/%d/%y %H:%M:%S') if row['datetime'] else None
                timestamp = date.strftime(TIMESTAMP_FORMAT)
                name, extension = os.path.splitext(file_path)
                filename = f'{timestamp}-{item_id}{extension}' if timestamp and extension else file_path.name

            # Download file
            target = f'{destination_dir}/{filename}'
            r = requests.get(url)
            with open(target, 'wb') as f:
                f.write(r.content)
            return item_id
    except Exception as e:
        print(f'Error downloading file with {id_column} {item_id}: ', e)
        return item_id


if __name__ == '__main__':
    try:
        # Get all csv files in directory; exclude download tracking files
        files = filter(lambda x: x.endswith(".csv"), os.listdir())

        # Read files
        for filename in files:
            with open(filename, mode='r') as file:
                start = time.time()
                content = csv.DictReader(file)
                rows = list(content)

                if 'url' not in content.fieldnames:
                    # File doesn't have url column - nothing to download
                    continue
                else:
                    print(f'[{filename}]: Extracting data from {filename}...')
                    total_count = len(rows)
                    print(f'[{filename}]: Total items - {total_count}')

                    # Create new directory
                    folder_name = Path(filename).stem
                    parent_dir = os.path.join(os.getcwd(), folder_name)
                    if not os.path.exists(parent_dir):
                        print(f'[{filename}]: Creating directory...')
                        os.makedirs(parent_dir)

                    # Get downloaded ids
                    tracking_file_name = f'{folder_name}.downloaded.txt'
                    id_column = f'{folder_name[:-1:]}_id'  # Get id column name from filename/folder (super naive way to get singular form)
                    download_interrupted = os.path.exists(tracking_file_name)
                    processed_ids = []
                    if download_interrupted:
                        with open(tracking_file_name, 'r') as tracking_file:
                            processed_ids = list(filter(lambda x: x != '', tracking_file.read().split(';')))
                        processed_count = len(processed_ids)

                        print(f'[{filename}]: Processed items from previous run(s): {processed_count}')
                        if processed_count == total_count:
                            print(f'[{filename}]: File(s) already downloaded.')
                            continue

                    # Create subdirectories
                    if 'site_id' in content.fieldnames:
                        site_ids = set([row['site_id'] for row in rows if row[id_column] not in processed_ids])
                        print(f'[{filename}]: Creating subdirectories for sites...')
                        for site_id in site_ids:
                            if site_id:
                                subdir = os.path.join(parent_dir, site_id)

                                # Create subdirectory for site id
                                if not os.path.exists(subdir):
                                    os.makedirs(subdir)

                    # Prepare function
                    download = functools.partial(fetch_file, parent_dir, id_column, processed_ids)

                    # Download files
                    print(f'[{filename}]: Reading data and downloading files from urls...')
                    with ThreadPoolExecutor() as executor:
                        futures = {executor.submit(download, row): row['url'] for row in rows}

                        # As results from each thread become available, push them to tracking file
                        with open(tracking_file_name, 'a') as tf:
                            for future in as_completed(futures):
                                result = future.result()
                                if result:
                                    tf.write(result)
                                    tf.write(';')

                    total_time = time.time() - start
                    print(
                        f'[{filename}]: Download for {filename} finished. Total time (in seconds): {round(total_time, 2)}')
    except KeyboardInterrupt:
        print('Script manually interrupted by user.')
        os._exit(0)
