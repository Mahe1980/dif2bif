import json
import logging
import os
from xml.etree import ElementTree

from dif2bif import constant
from .xml_converter import XMLConverter

logger = logging.getLogger()
logging.basicConfig(format='%(levelname)s:%(message)s', level=os.environ.get("LOG_LEVEL", "INFO"))


class CreateSchema:
    @staticmethod
    def __create_service_fields():
        service_fields = {'SERVICE_PROCESSED_AT': 'timestamp', 'SERVICE_FILE_ID': 'string'}
        service_fields_output = list()
        for key, value in service_fields.items():
            field_dict = dict()
            field_dict['name'] = key
            field_dict['mode'] = 'required'
            field_dict['type'] = value
            field_dict['description'] = key.lower().replace('_', ' ')

            service_fields_output.append(field_dict)
        return service_fields_output

    @staticmethod
    def __create_partition_fields(data_conversion):
        partition_fields_output = list()
        partitions = XMLConverter.findall_elements(data_conversion, 'partitions/')
        if partitions:
            partition_field_dict = dict()
            partition_field_dict['name'] = 'partition_year_month_day'
            partition_field_dict['type'] = 'timestamp'
            partition_field_dict['mode'] = 'required'
            partition_field_dict['description'] = 'partition standard column'

            partition_fields_output.append(partition_field_dict)
        return partition_fields_output

    @staticmethod
    def __create_schema_fields(data_conversion):
        schema_fields = XMLConverter.findall_elements(data_conversion, 'schema/')
        schema_fields_output = list()
        for schema_field in schema_fields:
            field_dict = dict()
            attr = schema_field.attrib

            field_dict['name'] = attr['name']

            if attr['type'] in ('DOUBLE', 'double', 'float', 'FLOAT'):
                data_type = 'numeric'
            elif attr['type'] in ('integer', 'INTEGER', 'long', 'LONG', 'bigint', 'BIGINT'):
                data_type = 'int64'
            elif attr['type'] in ('epoch', 'EPOCH'):
                data_type = 'timestamp'
            else:
                data_type = attr['type']

            field_dict['type'] = data_type

            if attr['required'] == 'false':
                field_dict['mode'] = 'nullable'
            else:
                field_dict['mode'] = 'required'

            field_dict['description'] = attr['name'].lower().replace('_', ' ')

            schema_fields_output.append(field_dict)
        return schema_fields_output

    @staticmethod
    def create_schema(xml_path, table_name):
        xml_file_path = os.path.join(xml_path, constant.OUTPUT_XML_FILE_NAME)
        logger.info(f'Creating schema for `{xml_file_path}`')
        try:
            xml = ElementTree.parse(xml_file_path)
            data_conversion = XMLConverter.find_element(xml, 'dataConversions/dataConversion')
            schema_fields = CreateSchema.__create_schema_fields(data_conversion)
            service_fields = CreateSchema.__create_service_fields()
            partition_fields = CreateSchema.__create_partition_fields(data_conversion)
            output_fields = schema_fields + service_fields + partition_fields

            target_path_uri = f'{xml_path}/{table_name}_schema.json'

            with open(target_path_uri, 'w') as fp:
                json.dump(output_fields, indent=4, separators=(',', ': '), fp=fp)
            logger.info(f"Schema created for data source table `{table_name}`")
        except Exception as ex:
            logger.error(f'Unable to create schema for table `{table_name}` due to `{ex}`')
