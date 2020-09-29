import argparse
import logging
import os
import sys

from dif2bif.xml_converter import XMLConverter

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def main(conversion_path, ingestion_path, output_path, mapping_file_path):
    xml_converter = XMLConverter()
    xml_converter.xml_converter(conversion_path, ingestion_path, output_path, mapping_file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--conversion_path", required=True, help="Enter absolute conversion path")
    parser.add_argument("--ingestion_path", required=True, help="Enter absolute ingestion path")
    parser.add_argument("--output_path", required=True, help="Enter absolute output path")
    parser.add_argument("--mapping_file_path", required=True, help="Enter absolute file path to mapping.csv")

    args = parser.parse_args()
    if args.output_path == args.conversion_path or args.output_path == args.ingestion_path:
        logger.error('Output path cannot be equal to conversion or ingestion path')
        sys.exit(1)

    main(args.conversion_path, args.ingestion_path, args.output_path, args.mapping_file_path)
