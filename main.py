import argparse
import os
import datetime
import shutil
import logging

import pandas as pd
import geopandas as gpd
from hdx.utilities.path import get_temp_dir

from utils.hdx_api import query_api


ACAPS_HDX_ADDRESS = 'acaps-covid19-government-measures-dataset'

CRASH_MOVE_MAIN_DIR = os.path.join('2020-03-16-global-covid-19-response-group', 'GIS')
CRASH_MOVE_INPUT_DIR = '1_Original_Data'
CRASH_MOVE_OUTPUT_DIR = '2_Active_Data'

ACAPS_DIR = 'ACAPS_Govt_Measures'

NATURAL_EARTH_DIR = os.path.join('NaturalEarth', 'NE_admin_wld')
NATURAL_EARTH_FILENAME = 'ne_10m_admin_0_countries_lakes'
#NATURAL_EARTH_COLNAMES = ['SOVEREIGNT', 'NAME', 'ADM_A3_IS']

OUTPUT_DIR = 'ToWeb'
OUTPUT_FILENAME = 'wrl_government_measures_pt_s0_acaps_pp_governmentmeasures.shp'
REDUCED_OUTPUT_FILENAME = 'wrl_government_measures_py_s0_acaps_pp_governmentmeasures_SummaryByMonth.shp'

logger = logging.getLogger()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmf_path', help='Path to crash move folder')
    parser.add_argument('-d', '--debug', action='store_true', help='Run without querying API')
    args = parser.parse_args()
    return args


def main(cmf_path: str, debug: bool = False):
    # Get ACAPS and Natural Earth data
    logger.info('Getting ACAPS and Natural Earth data')
    df_acaps = get_df_acaps(cmf_path, debug)
    df_acaps_reduced = get_df_acaps_reduced(df_acaps)
    df_naturalearth = get_df_naturalearth(cmf_path)
    # Do the join, and convert to point instead of shape
    logger.info('Joining data')
    df_output = get_df_output(df_acaps, df_naturalearth)
    df_output_reduced = join_naturalearth_with_acaps(df_naturalearth, df_acaps_reduced)
    # Output to CMF
    logger.info('Writing out to shape file')
    output_to_cmf(df_output, df_output_reduced, cmf_path)
    logger.info('Done')


def get_df_acaps(cmf_path: str, debug: bool) -> pd.DataFrame:
    # Get the ACAPS data from HDX
    data_dir = get_temp_dir('covid19_acaps')
    acaps_dir = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, CRASH_MOVE_INPUT_DIR, ACAPS_DIR)
    if not debug:
        filename_original = list(query_api(ACAPS_HDX_ADDRESS, data_dir).values())[0]
        filename = f'{datetime.date.today().strftime("%Y%m%d")}_{filename_original}'
        filepath = os.path.join(acaps_dir, filename)
        # Copy to crash move folder
        logger.info(f'Moving dataset to {filepath}')
        shutil.move(os.path.join(data_dir, filename_original), filepath)
    else:
        # If debug, check datadir folder and take the last item
        filename = sorted(os.listdir(acaps_dir))[-1]
    # Read in the dataframe
    df_acaps = pd.read_excel(os.path.join(acaps_dir, filename), sheet_name='Database',
                             parse_dates=['DATE_IMPLEMENTED'],
                             usecols=['REGION', 'COUNTRY', 'ISO', 'CATEGORY', 'MEASURE',
                                      'DATE_IMPLEMENTED', 'ID', 'LOG_TYPE'])
    # Drop rows with empty region
    df_acaps = df_acaps.loc[(df_acaps['REGION'] != '') & (~df_acaps['REGION'].isna())]
    # Make month column and onvert datetime column to string to write to shape file
    df_acaps['MONTH'] = df_acaps['DATE_IMPLEMENTED'].dt.strftime('%B')
    df_acaps['DATE_IMPLEMENTED'] = df_acaps['DATE_IMPLEMENTED'].dt.strftime('%Y-%m-%d')
    # Fill NAs in dates with "Unknown"
    df_acaps.loc[:, ['DATE_IMPLEMENTED', 'MONTH']] = df_acaps.loc[:, ['DATE_IMPLEMENTED', 'MONTH']].fillna('Unknown')
    return df_acaps


def get_df_acaps_reduced(df_acaps: pd.DataFrame) -> pd.DataFrame:
    return (df_acaps.assign(CAT_CNT=0)
                    .drop(columns=['DATE_IMPLEMENTED', 'MEASURE'])
                    .groupby(['COUNTRY', 'REGION',  'ISO', 'MONTH', 'CATEGORY', 'LOG_TYPE'])
                    .count().reset_index()
                     )


def get_df_naturalearth(cmf_path: str) -> gpd.GeoDataFrame:
    # TODO: Fetch NaturalEarth data?
    # Read in NaturalEarth data
    # TODO: Read in from original shapefile and add transformations here
    input_filename = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, CRASH_MOVE_INPUT_DIR,
                                  NATURAL_EARTH_DIR, NATURAL_EARTH_FILENAME)
    df_naturalearth = gpd.read_file(f'{input_filename}.shp')
    return df_naturalearth


def get_df_output(df_acaps: pd.DataFrame, df_naturalearth: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_output = join_naturalearth_with_acaps(df_naturalearth, df_acaps)
    # Convert to points
    df_output['geometry'] = df_output['geometry'].apply(lambda x: x.representative_point())
    # Rename columns for dashboard
    df_output = df_output.rename(columns={
        'CATEGORY': 'CATEGOR',
        'DATE_IMPLEMENTED': 'DATE_IM',
        'ADM0_A3_IS': 'ADM0_A3',
        'Shape_Leng': 'Shp_Lng',
        'Shape_Area': 'Shap_Ar',
        'SOVEREIGNT': 'SOVEREI',
        'ID': 'ORIG_FID'
    })

    return df_output


def join_naturalearth_with_acaps(df_naturalearth: gpd.GeoDataFrame, df_acaps: pd.DataFrame) -> gpd.GeoDataFrame:
    return df_naturalearth.merge(df_acaps, how='outer', left_on='ADM0_A3_IS', right_on='ISO').drop(['ISO'], axis=1)


def output_to_cmf(df_output, df_output_reduced, cmf_path):
    output_dir = os.path.join(cmf_path, CRASH_MOVE_MAIN_DIR, CRASH_MOVE_OUTPUT_DIR,
                              OUTPUT_DIR, datetime.date.today().strftime('%Y%m%d'))
    try:
        os.mkdir(output_dir)
    except FileExistsError:
        pass
    df_output.to_file(os.path.join(output_dir, OUTPUT_FILENAME))
    df_output_reduced.to_file(os.path.join(output_dir, REDUCED_OUTPUT_FILENAME))


if __name__ == '__main__':
    args = parse_args()
    main(args.cmf_path, debug=args.debug)
