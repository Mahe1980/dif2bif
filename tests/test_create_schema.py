import os
from pathlib import Path

import lxml.etree as element_tree
import pytest

from dif2bif import constant
from dif2bif.create_schema import CreateSchema

ROOT_DIR = Path(__file__).resolve().parent.parent
RESOURCES_FOLDER = os.path.join(ROOT_DIR, 'tests', 'resources')
XML_PATH_BSS = os.path.join(RESOURCES_FOLDER, 'expected', 'CKM_D_SIM_BDP_F_D')
XML_PATH_CKM = os.path.join(RESOURCES_FOLDER, 'expected', 'DM_CAMPAIGN_BDP_D_D')
XML_PATH_INVALID = os.path.join(RESOURCES_FOLDER, 'invalid', 'invalid')

OUTPUT_INVALID = os.path.join(XML_PATH_CKM, 'INVALID_schema.json')

test_create_schema_data = [(XML_PATH_BSS, 'CKM_D_SIM_BDP_F_D'),
                           (XML_PATH_CKM, 'DM_CAMPAIGN_BDP_D_D')]

test_create_schema_xml_not_found_data = [(XML_PATH_INVALID, 'INVALID', OUTPUT_INVALID)]
test_create_service_fields_data = [[
    {'name': 'SERVICE_PROCESSED_AT', 'mode': 'required', 'type': 'timestamp', 'description': 'service processed at'},
    {'name': 'SERVICE_FILE_ID', 'mode': 'required', 'type': 'string', 'description': 'service file id'}]]

test_create_partition_fields_data = [([{'name': 'partition_year_month_day', 'type': 'timestamp', 'mode': 'required',
                                        'description': 'partition standard column'}], XML_PATH_BSS),
                                     ([{'name': 'partition_year_month_day', 'type': 'timestamp', 'mode': 'required',
                                        'description': 'partition standard column'}], XML_PATH_CKM)
                                     ]

test_create_schema_fields_data = [
    ({'name': 'SIM_ID_PROFIL', 'type': 'string', 'mode': 'nullable', 'description': 'sim id profil'}, XML_PATH_BSS),
    ({'name': 'date_target', 'type': 'string', 'mode': 'nullable', 'description': 'date target'}, XML_PATH_CKM)]


@pytest.mark.parametrize("xml_path, table_name", test_create_schema_data)
def test_create_schema(xml_path, table_name):
    output_file = os.path.join(xml_path, f"{table_name}_schema.json")
    CreateSchema().create_schema(xml_path, table_name)
    assert os.path.isfile(output_file)


@pytest.mark.parametrize("xml_path, table_name, output", test_create_schema_xml_not_found_data)
def test_create_schema_xml_not_found(xml_path, table_name, output):
    CreateSchema().create_schema(xml_path, table_name)
    assert not os.path.isfile(output)


@pytest.mark.parametrize('expected_service_fields', test_create_service_fields_data)
def test_create_service_fields(expected_service_fields):
    output_service_fields = CreateSchema._CreateSchema__create_service_fields()
    assert expected_service_fields == output_service_fields


@pytest.mark.parametrize('expected_partition_fields, xml_path', test_create_partition_fields_data)
def test_create_partition_fields(expected_partition_fields, xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(os.path.join(xml_path, constant.OUTPUT_XML_FILE_NAME), parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    output_partition_fields = CreateSchema._CreateSchema__create_partition_fields(data_conversion)
    assert expected_partition_fields == output_partition_fields


@pytest.mark.parametrize('expected_schema_fields, xml_path', test_create_schema_fields_data)
def test_create_schema_fields(expected_schema_fields, xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(os.path.join(xml_path, constant.OUTPUT_XML_FILE_NAME), parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    output_schema_fields = CreateSchema._CreateSchema__create_schema_fields(data_conversion)
    assert expected_schema_fields in output_schema_fields
