import json
import logging
import os
from xml.etree import ElementTree

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from dif2bif import constant

logger = logging.getLogger()
logging.basicConfig(format='%(levelname)s:%(message)s', level=os.environ.get("LOG_LEVEL", "INFO"))


class BigQueryTable:
    def __init__(self, bq_client):
        self.bq_client = bq_client

    def get_dataset(self, dataset_name):
        dataset = None
        dataset_ref = self.bq_client.dataset(dataset_name)
        try:
            dataset = self.bq_client.get_dataset(dataset_ref)
            logger.info(f'Dataset `{dataset.dataset_id}` already exists')
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset = self.bq_client.create_dataset(dataset)
            logger.info(f'Dataset `{dataset.dataset_id}` created')
        except Exception as ex:
            logger.info(f"Unable to validate dataset `{dataset_name}` due to `{ex}`")

        return dataset

    @staticmethod
    def get_conversion_property_value(data_conversion, name):
        element = data_conversion.find(f'properties/property[@name="{name}"]')
        return element.attrib['value']

    @staticmethod
    def get_dataset_name(data_conversion):
        xpath = 'conversionActions/writeActions/writeAction/[@name="WriteToBigQueryAction"]/config/dataset'
        data_set = data_conversion.find(xpath)
        data_set_name = data_set.attrib['name']
        country = BigQueryTable.get_conversion_property_value(data_conversion, 'country')
        big_query_project = BigQueryTable.get_conversion_property_value(data_conversion, 'bigQueryProject')
        big_query_data_category = BigQueryTable.get_conversion_property_value(data_conversion, 'bigQueryDataCategory')

        data_set_name = data_set_name \
            .replace('${country}', country) \
            .replace('${bigQueryProject}', big_query_project) \
            .replace('${bigQueryDataCategory}', big_query_data_category)

        return data_set_name

    @staticmethod
    def __set_partition(bq_table, schema):
        time_partition = [field['name'] for field in schema if field['name'] == 'partition_year_month_day']
        bq_table.time_partitioning = bigquery.TimePartitioning(field=time_partition[0])

    def create_table(self, xml_path, table_name):
        xml = ElementTree.parse(os.path.join(xml_path, constant.OUTPUT_XML_FILE_NAME))
        data_conversion = xml.find('dataConversions/dataConversion')
        table_name_in_xml = self.get_conversion_property_value(data_conversion, 'interface')

        if table_name != table_name_in_xml:
            raise Exception('Table names don\'t match with config and conversion xml file')

        try:
            dataset_name = self.get_dataset_name(data_conversion)
            dataset = self.get_dataset(dataset_name)
            table_ref = dataset.table(table_name)

            schema_file = f"{xml_path}/{table_name}_schema.json"
            with open(schema_file) as data_file:
                schema = json.load(data_file)
                bq_table = bigquery.Table(table_ref, schema=schema)
                self.__set_partition(bq_table, schema)
                try:
                    self.bq_client.get_table(table_ref)
                    logger.info(f"Table `{table_name}` already exists")
                except NotFound:
                    self.bq_client.create_table(bq_table)
                    logger.info(f"Table `{table_name}` has been created successfully")
        except Exception as ex:
            logger.error(f'Unable to create table - {table_name} due to `{ex}`')
