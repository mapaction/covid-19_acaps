import argparse
import os

import pandas as pd
from hdx.utilities.path import get_temp_dir

from utils.hdx_api import query_api


ACAPS_HDX_ADDRESS = "acaps-covid19-government-measures-dataset"
ACAPS_DATASET_NAME = "20200326 ACAPS - COVID-19 Goverment Measures Dataset v2.xlsx"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='Run without querying API')
    args = parser.parse_args()
    return args


def main(debug):
    data_dir = get_temp_dir('covid19_acaps')
    if not debug:
        filepath = query_api(ACAPS_HDX_ADDRESS, data_dir)[ACAPS_DATASET_NAME]
    else:
        filepath = os.path.join(data_dir, f'{ACAPS_DATASET_NAME}.XLSX')
    df = pd.read_excel(filepath, sheet_name='Database')


if __name__ == '__main__':
    args = parse_args()
    main(debug=args.debug)
