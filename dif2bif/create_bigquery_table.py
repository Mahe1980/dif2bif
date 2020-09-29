import argparse
import os

from google.cloud import bigquery

from dif2bif.big_query_table import BigQueryTable
from dif2bif.utils.load_config import load_config


def main(base_path, config_file_path):
    project_id, table_to_relative_path_list = load_config(config_file_path)

    bq_client = bigquery.Client(project=project_id)
    bq_table = BigQueryTable(bq_client)

    for table_relative_path in table_to_relative_path_list:
        for table, relative_path in table_relative_path.items():
            full_path = os.path.join(base_path, relative_path)
            bq_table.create_table(full_path, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", required=True, help="Enter base path to BIF XMLs")
    parser.add_argument("--config_file_path", required=True, help="Enter absolute file path to config.yaml")

    args = parser.parse_args()
    main(args.base_path, args.config_file_path)
