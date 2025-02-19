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
import subprocess
from pydub import AudioSegment


TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
SONGTYPES = {
  '1': 'Common Song',
  '2': 'Courtship Song',
  '3': 'Territorial Song',
  '4': 'Simple Call',
  '5': 'Simple Call 2',
  '6': 'Alternative Song',
  '7': 'Alternative Song 2',
  '8': 'Mechanical Song',
  '9': 'Nocturnal Song',
}

def trim_audio(src, dest, start, end, ext):
  # Load the audio file
  audio = AudioSegment.from_file(src)

  # Trim the audio file (in milliseconds)
  start_ms = float("{:.3f}".format(start)) * 1000
  end_ms = float("{:.3f}".format(end)) * 1000
  trimmed_audio = audio[start_ms:end_ms]

  # Export the trimmed audio
  trimmed_audio.export(dest, format=ext)  # or the desired format


def fetch_file(parent_dir, processed_ids, row, detected_recordings, species_name):
    """ Downloads file from a given URL
    :param parent_dir: - default target directory
    :param row: - row from csv file, containing url
    :return:
    """
    destination_dir = parent_dir
    item_id = row['recording_id']
    url = row['url']

    if item_id in processed_ids:
        return None

    try:
        # Prepare sub-directories by following format
        # {species_name}/{present or absent}/{site_id}
        for det_rec in detected_recordings[row['recording_id']]:
          spc_name = species_name[det_rec['species_id']]
          validated = 'present' if det_rec['validated'] == '1' else 'absent'
          songtype = SONGTYPES[det_rec['songtype_id']].replace(' ', '_')
          if 'site_id' in row:
              site_id = row['site_id']
              destination_dir = os.path.join(parent_dir, f'{spc_name}_{songtype}', validated, site_id)
              if not os.path.exists(destination_dir):
                try:
                  os.makedirs(destination_dir)
                except Exception:
                  # just ignore in case it is trying to create same folder name due to thread overlap
                  pass

          if url:
              file_path = Path(urlparse(url).path)
              filename = file_path.name

              # Generate filename for recordings
              date = datetime.strptime(row['datetime'], '%m/%d/%Y %H:%M:%S') if row['datetime'] else None
              timestamp = date.strftime(TIMESTAMP_FORMAT)
              name, extension = os.path.splitext(file_path)
              filename_without_ex = f'{timestamp}-{item_id}'
              filename = f'{timestamp}-{item_id}{extension}' if timestamp and extension else file_path.name

              # Download file
              target = f'{parent_dir}/{filename}'
              r = requests.get(url)
              with open(target, 'wb') as f:
                  f.write(r.content)
                  
              # Trim only detect species
              x1 = float(det_rec['x1'])
              x2 = float(det_rec['x2'])
              new_target = f'{destination_dir}/{filename_without_ex}_{x1}_{x2}{extension}'
              trim_audio(target, new_target, x1, x2, extension[1:])
        Path(target).unlink()
        return item_id
    except Exception as e:
        print(f'Error downloading file with recording_id {item_id}: ', e)
        return item_id


if __name__ == '__main__':
    try:
        # Read Pattern matching and collect all data to dict
        pms = filter(lambda x: x.startswith("pattern_matching_rois"), os.listdir())
        detected_recordings = {}
        for pm in pms:
            with open(pm, mode='r') as file:
                content = csv.DictReader(file)
                rows = list(content)
                for row in rows:
                  if row['recording_id'] not in detected_recordings.keys():
                    detected_recordings[row['recording_id']] = []
                    detected_recordings[row['recording_id']].append({
                      'x1': row['x1'],
                      'x2': row['x2'],
                      'species_id': row['species_id'],
                      'validated': row['validated'],
                      'songtype_id': row['songtype_id']
                    })
                  else:
                    detected_recordings[row['recording_id']].append({
                      'x1': row['x1'],
                      'x2': row['x2'],
                      'species_id': row['species_id'],
                      'validated': row['validated'],
                      'songtype_id': row['songtype_id']
                    })
        
        # Read Species and collect all data to dict
        species = filter(lambda x: x.startswith("species"), os.listdir())
        species_name = {}
        for spc in species:
          with open(spc, mode='r') as file:
            content = csv.DictReader(file)
            rows = list(content)
            for row in rows:
              species_name[row['species_id']] = row['scientific_name'].replace(' ', '_')

        # Get all recordings csv files in directory; exclude download tracking files
        recs = filter(lambda x: x.endswith(".csv") and x.startswith("recordings"), os.listdir())
        for rec in recs:
            with open(rec, mode='r') as file:
                start = time.time()
                content = csv.DictReader(file)
                rows = list(content)

                if 'url' not in content.fieldnames:
                    # File doesn't have url column - nothing to download
                    continue
                else:
                    print(f'[{rec}]: Extracting data from {rec}...')
                    total_count = len(rows)
                    print(f'[{rec}]: Total items - {total_count}')

                    # Create new directory
                    folder_name = 'detected_recordings'
                    parent_dir = os.path.join(os.getcwd(), folder_name)
                    if not os.path.exists(parent_dir):
                        print(f'[{rec}]: Creating directory...')
                        os.makedirs(parent_dir)

                    # Get downloaded ids
                    tracking_file_name = f'{Path(rec).stem}.downloaded.txt'
                    download_interrupted = os.path.exists(tracking_file_name)
                    processed_ids = []
                    if download_interrupted:
                        with open(tracking_file_name, 'r') as tracking_file:
                            processed_ids = list(filter(lambda x: x != '', tracking_file.read().split(';')))
                        processed_count = len(processed_ids)

                        print(f'[{rec}]: Processed items from previous run(s): {processed_count}')
                        if processed_count == total_count:
                            print(f'[{rec}]: File(s) already downloaded.')
                            continue

                    # Prepare function
                    download = functools.partial(fetch_file, parent_dir, processed_ids)

                    # Download files
                    print(f'[{rec}]: Reading data and downloading files from urls...')
                    with ThreadPoolExecutor() as executor:
                        # Filter recording url that out of detected
                        futures = {executor.submit(download, row, detected_recordings, species_name): row['url'] for row in rows if row['recording_id'] in detected_recordings.keys()}

                        # As results from each thread become available, push them to tracking file
                        with open(tracking_file_name, 'a') as tf:
                            for future in as_completed(futures):
                                result = future.result()
                                if result:
                                    tf.write(result)
                                    tf.write(';')

                    total_time = time.time() - start
                    print(
                        f'[{rec}]: Download for {rec} finished. Total time (in seconds): {round(total_time, 2)}')
    except KeyboardInterrupt:
        print('Script manually interrupted by user.')
        os._exit(0)
