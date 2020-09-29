import json
import os
from pathlib import Path

import pytest

from dif2bif.create_bigquery_schema import main

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = os.path.join(ROOT_DIR, 'tests', 'resources', 'config.yaml')
PATH_BSS = os.path.join(ROOT_DIR, 'tests', 'resources', 'expected', 'CKM_D_SIM_BDP_F_D')
PATH_CKM = os.path.join(ROOT_DIR, 'tests', 'resources', 'expected', 'DM_CAMPAIGN_BDP_D_D')

test_create_big_query_schema_data = [(ROOT_DIR, CONFIG_PATH, PATH_BSS, 'CKM_D_SIM_BDP_F_D'),
                                     (ROOT_DIR, CONFIG_PATH, PATH_CKM, 'DM_CAMPAIGN_BDP_D_D')
                                     ]


@pytest.mark.parametrize("base_path, config_file_path, output_path, table_name",
                         test_create_big_query_schema_data)
def test_create_big_query_schema(base_path, config_file_path, output_path, table_name):
    with open(os.path.join(output_path, f"{table_name}_schema.json")) as json_file:
        expected_schema = json.load(json_file)

    main(base_path, config_file_path)

    with open(os.path.join(output_path, f"{table_name}_schema.json")) as json_file:
        actual_schema = json.load(json_file)

    assert actual_schema == expected_schema
