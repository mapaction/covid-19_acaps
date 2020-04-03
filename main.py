import argparse
import os
import datetime
import shutil

import pandas as pd
import geopandas as gpd
from hdx.utilities.path import get_temp_dir

from utils.hdx_api import query_api


ACAPS_HDX_ADDRESS = 'acaps-covid19-government-measures-dataset'

CRASH_MOVE_MAIN_DIR = os.path.join('2020-03-16-global-covid-19-response-group',
                                   'GIS',
                                   '1_Original_Data')

ACAPS_DIR = 'ACAPS_Govt_Measures'

NATURAL_EARTH_DIR = 'NaturalEarth'
NATURAL_EARTH_ZIP_FILENAME = 'NE_admin_wld'
NATURAL_EARTH_FILENAME = 'ne_10m_admin_0_countries_lakes'
#NATURAL_EARTH_COLNAMES = ['SOVEREIGNT', 'NAME', 'ADM_A3_IS']

OUTPUT_DIR = 'ToWeb'
OUTPUT_FILENAME = 'wrl_government_measures_py_s0_acaps_pp_governmentmeasures.shp'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmf_path', help='Path to crash move folder')
    parser.add_argument('-d', '--debug', action='store_true', help='Run without querying API')
    args = parser.parse_args()
    return args


def main(cmf_path, debug=False):
    # Get the ACAPS data from HDX
    data_dir = get_temp_dir('covid19_acaps')
    acaps_dir = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, ACAPS_DIR)
    if not debug:
        filename = list(query_api(ACAPS_HDX_ADDRESS, data_dir).values())[0]
        filepath = os.path.join(acaps_dir, filename)
        # Copy to crash move folder
        shutil.move(os.path.join(data_dir, filename), filepath)
    else:
        # If debug, check datadir folder and take the last item
        filename = sorted(os.listdir(acaps_dir))[-1]
    # Read in the dataframe
    df_acaps = pd.read_excel(os.path.join(acaps_dir, filename), sheet_name='Database')
    # TODO: Fetch NaturalEarth data?
    # Read in NaturalEarth data
    # TODO: Read in from original shapefile and add transformations here
    input_filename = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, NATURAL_EARTH_DIR, NATURAL_EARTH_ZIP_FILENAME)
    df_naturalearth = gpd.read_file(f'zip://{input_filename}.zip!{NATURAL_EARTH_FILENAME}.shp')
    # Do the join
    df_output = df_naturalearth.merge(df_acaps, how='outer', left_on='ADM0_A3_IS', right_on='ISO')
    # Convert datetime column to string to write to shape file
    for colname in ['DATE_IMPLEMENTED', 'ENTRY_DATE']:
        df_output[colname] = df_output[colname].dt.strftime('%Y-%m-%d')
    # Drop link because ESRI doesn't like it
    df_output = df_output.drop(columns=['LINK'])
    # Output to CMF
    output_dir = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, OUTPUT_DIR, datetime.date.today().strftime('%d%m'))
    try:
        os.mkdir(output_dir)
    except FileExistsError:
        pass
    df_output.to_file(os.path.join(output_dir, OUTPUT_FILENAME))


if __name__ == '__main__':
    args = parse_args()
    main(args.cmf_path, debug=args.debug)
