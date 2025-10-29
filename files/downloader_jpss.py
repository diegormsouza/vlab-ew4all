from datetime import datetime
import pandas as pd
import gcsfs
from google.cloud import storage
import os
import re
from concurrent.futures import ThreadPoolExecutor

def download_jpss_sdr(bucket, band, start, end, download_dir='/content/jpss_samples', include_checksums=False):
    """
    Download VIIRS SDR and geolocation files for a specified band and time range.

    Parameters:
    - bucket: Name of the Google Cloud Storage bucket (e.g., 'gcp-noaa-nesdis-n21')
    - band: VIIRS band (e.g., 'I1', 'M3', 'M5', 'DNB')
    - start: Start time in 'YYYY-MM-DD HH:MM' format
    - end: End time in 'YYYY-MM-DD HH:MM' format
    - download_dir: Directory to store downloaded files (default: '/content/jpss_samples')
    - include_checksums: Whether to download .sha384 checksum files (default: False)

    Returns:
    - List of local file paths (SDR and geolocation, excluding checksums unless specified)
    """
    bucket_name = bucket
    target_data = "VIIRS-DNB-SDR" if band == "DNB" else f"VIIRS-{band}-SDR"
    target_data_geo = ["VIIRS-DNB-GEO"] if band == "DNB" else (["VIIRS-MOD-GEO-TC"] if band.startswith('M') else ["VIIRS-IMG-GEO-TC"])

    # Parse start and end times
    year = start[0:4]
    month = start[5:7]
    day = start[8:10]
    start_hour = start[11:13]
    start_minute = start[14:16]
    end_hour = end[11:13]
    end_minute = end[14:16]

    start_limiter = datetime(int(year), int(month), int(day), int(start_hour), int(start_minute), 0)
    end_limiter = datetime(int(year), int(month), int(day), int(end_hour), int(end_minute), 0)
    fs = gcsfs.GCSFileSystem(anon=True)
    storage_client = storage.Client()

    # Ensure download directory exists
    os.makedirs(download_dir, exist_ok=True)

    def list_blobs(bucket_name, prefix, delimiter=None):
        try:
            blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
            return [blob.name for blob in blobs if include_checksums or not blob.name.endswith('.sha384')]
        except Exception as e:
            print(f"Error listing blobs for {prefix}: {e}")
            return []

    def parse_dates_sdr(s):
        try:
            if band == "DNB":
                match = re.search(r'd(\d{8})_t(\d{7})', s)
                if not match:
                    print(f"Invalid DNB file name format for {s}")
                    return None
                date_str, time_str = match.groups()
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                hours = int(time_str[:2])
                minutes = int(time_str[2:4])
                seconds = int(time_str[4:6])
                return datetime(year, month, day, hours, minutes, seconds)  # Seconds precision
            else:
                s = s.split("SDR/")[1].split(".h5")[0]
                s = s[21:38]
                year = int(s[1:5])
                month = int(s[5:7])
                day = int(s[7:9])
                hour = int(s[11:13])
                minute = int(s[13:15])
                seconds = int(s[15:17])
                return datetime(year, month, day, hour, minute, seconds)
        except Exception as e:
            print(f"Error parsing SDR date for {s}: {e}")
            return None

    def parse_dates_geo(s):
        try:
            match = re.search(r'd(\d{8})_t(\d{6,7})', s)  # Match 6 or 7 digits for time
            if not match:
                print(f"Invalid GEO file name format for {s}")
                return None
            date_str, time_str = match.groups()
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            hours = int(time_str[:2])
            minutes = int(time_str[2:4])
            seconds = int(time_str[4:6])
            return datetime(year, month, day, hours, minutes, seconds)  # Seconds precision
        except Exception as e:
            print(f"Error parsing GEO date for {s}: {e}")
            return None

    def download_blob(args):
        bucket_name, source_blob_name, destination_file_name = args
        try:
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)
            print(f'Blob {source_blob_name} downloaded to {destination_file_name}.')
        except Exception as e:
            print(f"Error downloading {source_blob_name}: {e}")

    # Download SDR files
    results = list_blobs(bucket_name, f"{target_data}/{year}/{month}/{day}/")
    print(f'Number of {target_data} files: {len(results)}')
    dfr = pd.DataFrame(results, columns=['Files'])
    dfr['Date'] = dfr.Files.apply(parse_dates_sdr)
    dfr = dfr.dropna(subset=['Date'])
    lets_get = dfr[(dfr.Date >= start_limiter) & (dfr.Date < end_limiter)][['Files', 'Date']]
    print(f'Filtered {target_data} to: {len(lets_get)}')

    # Download GEO files
    geo_files = []
    for geo_type in target_data_geo:
        results_geo = list_blobs(bucket_name, f"{geo_type}/{year}/{month}/{day}/")
        print(f'Number of {geo_type} files: {len(results_geo)}')
        dfr_geo = pd.DataFrame(results_geo, columns=['Files'])
        dfr_geo['Date'] = dfr_geo.Files.apply(parse_dates_geo)
        dfr_geo = dfr_geo.dropna(subset=['Date'])
        print(f"Filtered {geo_type} files in range: {len(dfr_geo[(dfr_geo.Date >= start_limiter) & (dfr_geo.Date < end_limiter)])}")
        geo_files.extend(dfr_geo[(dfr_geo.Date >= start_limiter) & (dfr_geo.Date < end_limiter)][['Files', 'Date']].to_dict('records'))

    # Debug: Print available GEO timestamps
    print("Available GEO timestamps:", [geo['Date'] for geo in geo_files])
    print("Expected SDR timestamps:", lets_get['Date'].tolist())

    # Match SDR and GEO files by timestamp
    matched_files = []
    for sdr in lets_get.to_dict('records'):
        sdr_time = sdr['Date']
        matched_geo = [geo['Files'] for geo in geo_files if geo['Date'] == sdr_time]
        if matched_geo:
            matched_files.append(sdr['Files'])
            matched_files.extend(matched_geo)
        else:
            print(f"Warning: No matching GEO file for {sdr['Files']} at timestamp {sdr_time}")

    # Remove duplicates while preserving order
    matched_files = list(dict.fromkeys(matched_files))

    # Download files in parallel
    download_tasks = []
    local_files = []
    for file in matched_files:
        file_name = file.rsplit('/', 1)[-1]
        local_path = f'{download_dir}/{file_name}'
        if local_path not in local_files:
            local_files.append(local_path)
            if os.path.isfile(local_path):
                print(f"The file {file_name} already exists.")
            else:
                download_tasks.append((bucket_name, file, local_path))

    if download_tasks:
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(download_blob, download_tasks)

    # Verify all files exist
    local_files = [f for f in local_files if os.path.isfile(f) and not f.endswith('.sha384')]
    return local_files
