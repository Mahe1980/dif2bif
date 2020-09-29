import json
import os
from pathlib import Path
from unittest import mock
from xml.etree import ElementTree

import pytest
from google.cloud.exceptions import NotFound

from dif2bif import constant
from dif2bif.big_query_table import BigQueryTable

ROOT_DIR = Path(__file__).resolve().parent.parent
EXPECTED_DIR = os.path.join(ROOT_DIR, 'tests', 'resources', 'expected')
XML_PATH_BSS = os.path.join(EXPECTED_DIR, 'CKM_D_SIM_BDP_F_D')
XML_PATH_CKM = os.path.join(EXPECTED_DIR, 'DM_CAMPAIGN_BDP_D_D')

XML_FILE_NAME_BSS = os.path.join(XML_PATH_BSS, constant.OUTPUT_XML_FILE_NAME)
XML_FILE_NAME_CSK = os.path.join(XML_PATH_CKM, constant.OUTPUT_XML_FILE_NAME)

test_create_big_query_table_data = [(XML_PATH_CKM, 'DM_CAMPAIGN_BDP_D_D')]

test_get_property_element_data = [(XML_FILE_NAME_BSS, 'CKM_D_SIM_BDP_F_D', 'it', 'dh', 'ckm'),
                                  (XML_FILE_NAME_CSK, 'DM_CAMPAIGN_BDP_D_D', 'it', 'dh', 'ckm')]

test_get_dataset_name_data = [(XML_FILE_NAME_BSS, 'vfit_dh_lake_bss_rawprepared_s'),
                              (XML_FILE_NAME_CSK, 'vfit_dh_lake_ckm_rawprepared_s')]

test_create_table_data = [
    (XML_PATH_BSS, 'vfit_dh_lake_bss_rawprepared_s', 'CKM_D_SIM_BDP_F_D'),
    (XML_PATH_CKM, 'vfit_dh_lake_ckm_rawprepared_s', 'DM_CAMPAIGN_BDP_D_D')]

test_get_dataset_data = ['vfit_dh_lake_ckm_rawprepared_s']


@pytest.mark.parametrize(
    "xml_path, expected_table_name, expected_country, expected_big_query_project, expected_big_query_data_category",
    test_get_property_element_data)
def get_property_element(xml_path, expected_table_name, expected_country, expected_big_query_project,
                         expected_big_query_data_category):
    xml = ElementTree.parse(xml_path)
    data_conversion = xml.find('dataConversions/dataConversion')

    table_name_in_xml = BigQueryTable.get_conversion_property_value(data_conversion, 'interface')
    assert table_name_in_xml == expected_table_name

    country_in_xml = BigQueryTable.get_conversion_property_value(data_conversion, 'country')
    assert country_in_xml == expected_country

    big_query_project_in_xml = BigQueryTable.get_conversion_property_value(data_conversion, 'bigQueryProject')
    assert big_query_project_in_xml == expected_big_query_project

    big_query_data_category_in_xml = BigQueryTable.get_conversion_property_value(data_conversion,
                                                                                 'bigQueryDataCategory')
    assert big_query_data_category_in_xml == expected_big_query_data_category


@pytest.mark.parametrize("xml_path, expected_dataset", test_get_dataset_name_data)
def test_get_dataset_name(xml_path, expected_dataset):
    xml = ElementTree.parse(xml_path)
    data_conversion = xml.find('dataConversions/dataConversion')
    dataset_in_xml = BigQueryTable.get_dataset_name(data_conversion)
    assert dataset_in_xml == expected_dataset


@pytest.mark.parametrize('dataset_name', test_get_dataset_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_dataset_dataset_found(mock_client, dataset_name):
    dataset = BigQueryTable(mock_client).get_dataset(dataset_name)

    mock_client.dataset.assert_called_with(dataset_name)
    mock_client.get_dataset.assert_called_with(mock_client.dataset.return_value)
    assert dataset is not None


@pytest.mark.parametrize('dataset_name', test_get_dataset_data)
@mock.patch("google.cloud.bigquery.Dataset")
@mock.patch("google.cloud.bigquery.Client")
def test_get_dataset_dataset_not_found(mock_client, mock_dataset, dataset_name):
    mock_client.get_dataset.side_effect = NotFound('')

    dataset = BigQueryTable(mock_client).get_dataset(dataset_name)

    mock_client.dataset.assert_called_with(dataset_name)
    mock_client.get_dataset.assert_called_with(mock_client.dataset.return_value)
    mock_client.create_dataset.assert_called_with(mock_dataset.return_value)

    assert dataset is not None


@pytest.mark.parametrize('xml_path, dataset_name, table_name', test_create_table_data)
@mock.patch("google.cloud.bigquery.Table")
@mock.patch("google.cloud.bigquery.Client")
def test_create_table_table_found(mock_client, mock_table, xml_path, dataset_name, table_name):
    mock_client.get_table.side_effect = NotFound('')

    schema_file = f"{xml_path}/{table_name}_schema.json"
    with open(schema_file) as data_file:
        schema = json.load(data_file)

        BigQueryTable(mock_client).create_table(xml_path, table_name)

        mock_client.dataset.assert_called_with(dataset_name)
        mock_client.get_dataset.assert_called_with(mock_client.dataset.return_value)
        mock_table.assert_called_with(mock_client.get_dataset().table.return_value, schema=schema)
        mock_client.get_table.assert_called_with(mock_client.get_dataset().table.return_value)
        mock_client.create_table.assert_called_with(mock_table.return_value)


@pytest.mark.parametrize('xml_path, dataset_name, table_name', test_create_table_data)
@mock.patch("google.cloud.bigquery.Table")
@mock.patch("google.cloud.bigquery.Client")
def test_create_table_table_not_found(mock_client, mock_table, xml_path, dataset_name, table_name):
    schema_file = f"{xml_path}/{table_name}_schema.json"
    with open(schema_file) as data_file:
        schema = json.load(data_file)

        BigQueryTable(mock_client).create_table(xml_path, table_name)

        mock_client.dataset.assert_called_with(dataset_name)
        mock_client.get_dataset.assert_called_with(mock_client.dataset.return_value)
        mock_table.assert_called_with(mock_client.get_dataset().table.return_value, schema=schema)
        mock_client.get_table.assert_called_with(mock_client.get_dataset().table.return_value)
