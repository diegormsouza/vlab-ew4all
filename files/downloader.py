import os
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
from botocore import UNSIGNED

def download_GOES(timestamp, satellite, product, band, path_dest, scan_params=None, force_download=False, check_adjacent_hours=False):
    """
    Download files from NOAA GOES-16, GOES-17, GOES-18, or GOES-19 S3 bucket for a given date, time, satellite, product, and band.
    
    Args:
        timestamp (str): Date and time in format 'YYYY-MM-DD HH:MM' (e.g., '2025-10-08 12:00') for ABI/SUVI/SEIS/MAG; 
                         'YYYY-MM-DD HH:MM:SS' (e.g., '2025-10-08 12:00:20') for GLM.
        satellite (str): Satellite identifier ('G16', 'G17', 'G18', or 'G19' for GOES-16, 17, 18, or 19).
        product (str): Product type (e.g., 'ABI-L2-CMIPM', 'ABI-L1b-RadM', 'GLM-L2-LCFA', 'SUVI-L1b-Fe093', 'SEIS-L1b-EHIS', 'MAG-L1b-GEOF').
        band (int or str): ABI band number (e.g., 13) for band-specific products, or None for non-band products (e.g., GLM-L2-LCFA, SUVI-L1b-Fe093, SEIS-L1b-EHIS, MAG-L1b-GEOF).
        path_dest (str): Local directory to save the file.
        scan_params (dict, optional): Override scan settings, e.g., {'mode': '3', 'sector': 'M1'} for Mode 3, Meso 1 (ignored for GLM/SUVI/SEIS/MAG).
        force_download (bool, optional): If True, re-download files even if they exist locally. Default: False.
        check_adjacent_hours (bool, optional): If True and no SUVI/SEIS/MAG files are found for the exact minute, check adjacent hours (±1 hour). Default: False.
    
    Returns:
        str or list or None: Path of the downloaded file (str) if one file is downloaded (e.g., './samples/file.nc' for ABI/GLM/MAG),
                            list of paths (e.g., ['path/to/file.nc', 'path/to/file.fits']) if multiple files are downloaded (e.g., SUVI),
                            or None if no files match the exact minute (SUVI/SEIS/MAG/ABI), closest within ±2 minutes (CONUS), or seconds (GLM).
    """
    def fetch_s3_objects(bucket_name, prefix, s3_client):
        all_objects = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    all_objects.extend(page['Contents'])
        except Exception as e:
            print(f"Error listing S3 objects for {bucket_name}/{prefix}: {e}")
        return all_objects

    valid_satellites = ['G16', 'G17', 'G18', 'G19']
    if satellite not in valid_satellites:
        print(f"Invalid satellite: {satellite}. Must be one of {valid_satellites}.")
        return None

    satellite_num = satellite[1:]
    os.makedirs(path_dest, exist_ok=True)

    # Parse input timestamp
    has_seconds = len(timestamp.replace('-', '').replace(' ', '').replace(':', '')) == 14  # YYYYMMDDHHMMSS
    try:
        if has_seconds:
            dt_input = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            timestamp_prefix = dt_input.strftime('%Y%j%H%M%S')
        else:
            dt_input = datetime.strptime(timestamp + ':00', '%Y-%m-%d %H:%M:%S')
            timestamp_prefix = dt_input.strftime('%Y%j%H%M')
    except ValueError as e:
        print(f"Invalid date format: {timestamp}. Expected 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS' for GLM.")
        return None

    year = dt_input.strftime('%Y')
    day_of_year = dt_input.strftime('%j')
    hour = dt_input.strftime('%H')

    bucket_name = f'noaa-goes{satellite_num}'
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    mode = scan_params.get('mode', '6') if scan_params else '6'
    sector = scan_params.get('sector', None) if scan_params else None

    # Check product type
    is_mesoscale = product.endswith('M')
    is_conus = product.endswith('C') and not is_mesoscale
    is_glm_product = product == 'GLM-L2-LCFA'
    is_suvi_product = product.startswith('SUVI-L1b-')
    is_seis_product = product.startswith('SEIS-L1b-')
    is_mag_product = product == 'MAG-L1b-GEOF'

    # Validate timestamp format for GLM
    if is_glm_product and not has_seconds:
        print(f"Warning: GLM product {product} prefers 'YYYY-MM-DD HH:MM:SS' format; using 20-second window for {timestamp}.")

    # Define prefixes to check
    prefixes = [f'{product}/{year}/{day_of_year}/{hour}/']
    if (is_suvi_product or is_seis_product or is_mag_product) and check_adjacent_hours:
        # Add previous and next hour
        dt_prev = dt_input - timedelta(hours=1)
        dt_next = dt_input + timedelta(hours=1)
        prefixes.extend([
            f'{product}/{dt_prev.strftime("%Y")}/{dt_prev.strftime("%j")}/{dt_prev.strftime("%H")}/',
            f'{product}/{dt_next.strftime("%Y")}/{dt_next.strftime("%j")}/{dt_next.strftime("%H")}/'
        ])

    all_objects = []
    for prefix in prefixes:
        objects = fetch_s3_objects(bucket_name, prefix, s3_client)
        all_objects.extend(objects)

    if not all_objects:
        print(f'No files found for product: {product}, date: {timestamp}, GOES-{satellite_num}')
        return None

    # Check if product is band-specific (ABI products)
    is_band_specific = False
    for obj in all_objects:
        file_name = obj['Key'].split('/')[-1]
        for m in range(1, 7):
            for b in range(1, 17):
                if f'-M{m}C{b:02d}' in file_name:
                    is_band_specific = True
                    break
            if is_band_specific:
                break
        if is_band_specific:
            break

    if is_band_specific:
        if band is None:
            print(f"Warning: Product {product} requires a band number (e.g., 13). Trying first available band.")
            band = '01'
        band_str = f'C{int(band):02d}'
    else:
        if band is not None:
            print(f"Warning: Product {product} does not use bands; ignoring band={band}.")
        band_str = ''

    # Check product type
    if is_glm_product:
        if sector is not None:
            print(f"Warning: GLM product {product} does not use sectors; ignoring sector={sector}.")
        if band is not None:
            print(f"Warning: GLM product {product} does not use bands; ignoring band={band}.")
        sectors = ['']  # GLM has no sectors
        band_str = ''
    elif is_suvi_product or is_seis_product or is_mag_product:
        if sector is not None:
            print(f"Warning: {'SUVI' if is_suvi_product else 'SEIS' if is_seis_product else 'MAG'} product {product} does not use sectors; ignoring sector={sector}.")
        if band is not None:
            print(f"Warning: {'SUVI' if is_suvi_product else 'SEIS' if is_seis_product else 'MAG'} product {product} does not use bands; ignoring band={band}.")
        sectors = ['']  # SUVI/SEIS/MAG have no sectors
        band_str = ''
    elif is_mesoscale:
        sectors = [sector] if sector in ['M1', 'M2'] else ['M1', 'M2']
    else:
        sectors = ['']  # Non-mesoscale ABI products (e.g., CMIPF, CMIPC)

    possible_modes = [mode, '3', '4', '6'] if not (is_glm_product or is_suvi_product or is_seis_product or is_mag_product) else ['']  # No modes for GLM/SUVI/SEIS/MAG
    matching_files = []  # List to store both .nc and .fits for SUVI, or multiple .nc for SEIS/MAG/ABI
    used_mode = mode if not (is_glm_product or is_suvi_product or is_seis_product or is_mag_product) else ''
    used_sector = ''
    attempted_patterns = []
    closest_file = None
    closest_time_diff = timedelta(minutes=10)  # Initialize with max allowed for non-CONUS

    for try_mode in possible_modes:
        for sector in sectors:
            if is_mesoscale:
                product_with_sector = f'{product[:-1]}{sector}'  # e.g., 'ABI-L2-CMIPM' -> 'ABI-L2-CMIPM1'
            else:
                product_with_sector = product
            target_pattern = f'OR_{product_with_sector}{"-M" + try_mode if try_mode else ""}{band_str}_G{satellite_num}_s'
            attempted_patterns.append(target_pattern)

            for obj in all_objects:
                key = obj['Key']
                file_name_full = key.split('/')[-1]
                if not file_name_full.startswith(target_pattern):
                    continue

                # Extract timestamp from filename
                timestamp_start = key.find('_s') + 2
                timestamp_end = key.find('_e', timestamp_start)
                if timestamp_end == -1:
                    continue
                file_timestamp_full = key[timestamp_start : timestamp_end]
                if len(file_timestamp_full) < 14:
                    continue
                file_timestamp_str = file_timestamp_full[:-1]  # Remove decimal

                # Matching logic based on product type
                if is_glm_product:
                    # GLM: Match exact timestamp (including seconds) or closest within 20 seconds if no seconds provided
                    file_timestamp_prefix = file_timestamp_str[:13] if has_seconds else file_timestamp_str[:11]
                    if has_seconds:
                        if file_timestamp_prefix == timestamp_prefix:
                            matching_files = [key]  # Only one file (.nc) for GLM
                            used_sector = sector
                            break
                    else:
                        try:
                            dt_file = datetime.strptime(file_timestamp_str, '%Y%j%H%M%S')
                            time_diff = abs(dt_file - dt_input)
                            if time_diff <= timedelta(seconds=20):  # Closest within 20 seconds
                                if not matching_files or time_diff < timedelta(seconds=20):
                                    matching_files = [key]
                                    used_sector = sector
                        except ValueError:
                            continue
                elif is_conus:
                    # CONUS: Find closest file within ±2 minutes
                    try:
                        dt_file = datetime.strptime(file_timestamp_str, '%Y%j%H%M%S')
                        time_diff = abs(dt_file - dt_input)
                        if time_diff <= timedelta(minutes=2):  # Within ±2 minutes
                            if closest_file is None or time_diff < closest_time_diff:
                                closest_file = key
                                closest_time_diff = time_diff
                                used_mode = try_mode
                                used_sector = sector
                    except ValueError:
                        continue
                elif is_suvi_product or is_seis_product or is_mag_product or product.startswith('ABI-'):
                    # SUVI, SEIS, MAG, and other ABI: Match exact minute
                    file_timestamp_prefix = file_timestamp_str[:11]  # e.g., '20252811200'
                    if file_timestamp_prefix == timestamp_prefix[:11]:
                        matching_files.append(key)  # Collect .nc and .fits for SUVI, .nc for SEIS/MAG/ABI
                    else:
                        continue
                else:
                    # Other products: Find closest within 10 minutes
                    try:
                        dt_file = datetime.strptime(file_timestamp_str, '%Y%j%H%M%S')
                        time_diff = abs(dt_file - dt_input)
                        if time_diff < timedelta(minutes=10):
                            if not matching_files or time_diff < closest_time_diff:
                                matching_files = [key]
                                closest_time_diff = time_diff
                                used_mode = try_mode
                                used_sector = sector
                    except ValueError:
                        continue

            if matching_files or closest_file:
                break
        if matching_files or closest_file:
            break

    # Combine matching files and closest file for CONUS
    if is_conus and closest_file:
        matching_files = [closest_file]
    elif not matching_files and not closest_file:
        print(f"No matching file found for product: {product}, band: {band}, date: {timestamp}, GOES-{satellite_num}")
        print(f"Debug: Attempted patterns: {attempted_patterns}")
        if is_mesoscale and sector:
            print(f"Note: Specified sector {sector} may not have band {band} files. Try omitting sector or using 'M2'.")
        return None

    if used_mode != mode and not (is_glm_product or is_suvi_product or is_seis_product or is_mag_product):
        print(f"Note: Used mode M{used_mode} instead of default M{mode} for better match.")

    # Download all matching files
    downloaded_files = []
    for matching_file in matching_files:
        file_name = matching_file.split('/')[-1]
        file_path = os.path.join(path_dest, file_name)

        # Check if file exists
        if os.path.exists(file_path) and not force_download:
            print(f'File {file_path} already exists')
            downloaded_files.append(file_path)
            continue

        # Download the file
        sector_str = f"{used_sector}, " if used_sector else ""
        mode_str = f"mode M{used_mode}" if used_mode else ""
        print(f'Downloading file {file_path} from GOES-{satellite_num}')
        try:
            s3_client.download_file(bucket_name, matching_file, file_path)
            print(f'Successfully downloaded {file_path}')  # Success message
            downloaded_files.append(file_path)
        except Exception as e:
            print(f"Error downloading {file_name} from GOES-{satellite_num}: {e}")

    # Return single file path if only one file, list if multiple, or None if empty
    if not downloaded_files:
        return None
    elif len(downloaded_files) == 1:
        return downloaded_files[0]
    else:
        return downloaded_files