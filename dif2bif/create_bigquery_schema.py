import argparse
import os

from dif2bif.create_schema import CreateSchema
from dif2bif.utils.load_config import load_config


def main(base_path, config_file_path):
    _, table_to_relative_path_list = load_config(config_file_path)

    schema = CreateSchema()

    for table_to_relative_path in table_to_relative_path_list:
        for table, relative_path in table_to_relative_path.items():
            xml_path = os.path.join(base_path, relative_path)
            schema.create_schema(xml_path, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", required=True, help="Enter base path to BIF XMLs")
    parser.add_argument("--config_file_path", required=True, help="Enter absolute file path to config.yaml")

    args = parser.parse_args()
    main(args.base_path, args.config_file_path)
