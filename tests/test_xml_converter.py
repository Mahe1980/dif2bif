import os
import re
from pathlib import Path

import lxml.etree as element_tree
import pandas as pd
import pytest
from xmldiff import main

from dif2bif import constant
from dif2bif.xml_converter import XMLConverter

ROOT_DIR = Path(__file__).resolve().parent.parent
RESOURCES_PATH = os.path.join(ROOT_DIR, 'tests', 'resources')
MAPPING_PATH = os.path.join(RESOURCES_PATH, 'mapping.csv')
INPUT_XML_PATH = os.path.join(RESOURCES_PATH, 'input_xmls')
EXPECTED_DIR = os.path.join(RESOURCES_PATH, 'expected')
XML_FILE_PATH_CKM_D_SIM_BDP_F_D = os.path.join(INPUT_XML_PATH, 'CKM_D_SIM_BDP_F_D', constant.OUTPUT_XML_FILE_NAME)
XML_FILE_PATH_ZERO_EURO_THRESHOLD_CROSSING = os.path.join(INPUT_XML_PATH, 'ZERO_EURO_THRESHOLD_CROSSING',
                                                          constant.OUTPUT_XML_FILE_NAME)
XML_FILE_PATH_ES_CVM_IN_EXTR_DTOS_RIESG_FINPR_M_DATALAB_M = os.path.join(INPUT_XML_PATH,
                                                                         'ES_CVM_IN_EXTR_DTOS_RIESG_FINPR_M_DATALAB_M',
                                                                         constant.OUTPUT_XML_FILE_NAME)
XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D = os.path.join(INPUT_XML_PATH, 'DM_CAMPAIGN_BDP_D_D', constant.OUTPUT_XML_FILE_NAME)
XML_FILE_PATH_PI_INP = os.path.join(INPUT_XML_PATH, 'pi_inp', constant.OUTPUT_XML_FILE_NAME)
XML_FILE_PATH_INTERACTION_CONTENT = os.path.join(INPUT_XML_PATH, 'interaction_content', constant.OUTPUT_XML_FILE_NAME)
OUTPUT_PATH = os.path.join(ROOT_DIR, 'target', 'test_output')

test_xml_converter_data = [
    (INPUT_XML_PATH, INPUT_XML_PATH, OUTPUT_PATH, MAPPING_PATH, 'bss', 'CKM_D_SIM_BDP_F_D'),
    (INPUT_XML_PATH, INPUT_XML_PATH, OUTPUT_PATH, MAPPING_PATH, 'meg', 'ZERO_EURO_THRESHOLD_CROSSING'),
    (INPUT_XML_PATH, INPUT_XML_PATH, OUTPUT_PATH, MAPPING_PATH, 'cvm',
     'ES_CVM_IN_EXTR_DTOS_RIESG_FINPR_M_DATALAB_M'),
    (INPUT_XML_PATH, '', OUTPUT_PATH, MAPPING_PATH, 'ckm', 'DM_CAMPAIGN_BDP_D_D'),
    (INPUT_XML_PATH, '', OUTPUT_PATH, MAPPING_PATH, 'customer_interaction', 'interaction_content'),
    (INPUT_XML_PATH, '', OUTPUT_PATH, MAPPING_PATH, 'cvm', 'WEBMEGAPROD'),
    (INPUT_XML_PATH, '', OUTPUT_PATH, MAPPING_PATH, 'ckm', 'INVALID')]
test_update_conversion_actions_data = [
    (XML_FILE_PATH_CKM_D_SIM_BDP_F_D, "vfit_dh_lake_ckm_rawprepared_s"),
    (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, "vfit_dh_lake_ckm_rawprepared_s")]
test_update_conversion_properties_data = [
    (XML_FILE_PATH_CKM_D_SIM_BDP_F_D, "vfit_dh_lake_ckm_rawprepared_s"),
    (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, "vfit_dh_lake_ckm_rawprepared_s")]
test_input_files = [XML_FILE_PATH_CKM_D_SIM_BDP_F_D, XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, XML_FILE_PATH_PI_INP,
                    XML_FILE_PATH_INTERACTION_CONTENT]
test_update_write_actions_data = [(XML_FILE_PATH_CKM_D_SIM_BDP_F_D, 'CKM_D_SIM_BDP_F_D'),
                                  (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, 'DM_CAMPAIGN_BDP_D_D'),
                                  (XML_FILE_PATH_PI_INP, 'pi_inp')]
test_update_general_configuration_data = [(XML_FILE_PATH_CKM_D_SIM_BDP_F_D, 'it', 'vfit-dh-ckm-rawprepared'),
                                          (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, 'it', 'vfit-dh-ckm-rawprepared'),
                                          (XML_FILE_PATH_PI_INP, 'pt', 'vfpt-dh-ckm-rawprepared')]
test_read_mapping_file_mapping_data = [MAPPING_PATH]
test_read_conversion_xml_config_data = [(INPUT_XML_PATH, MAPPING_PATH)]
test_read_conversion_xml_data = [XML_FILE_PATH_CKM_D_SIM_BDP_F_D]
test_read_ingestion_xml_data = [(INPUT_XML_PATH, "3fc8e5b7-be0c-489e-a62c-e3dcf601673b"),
                                (INPUT_XML_PATH, "f1435656-6dd1-4413-b84c-ddb6c57cfaa5")]
test_update_partitions_data = [
    (XML_FILE_PATH_CKM_D_SIM_BDP_F_D, 'CKM_D_SIM_BDP_F_D', 'f1435656-6dd1-4413-b84c-ddb6c57cfaa5',
     INPUT_XML_PATH),
    (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, 'DM_CAMPAIGN_BDP_D_D', '', '.'),
    (XML_FILE_PATH_PI_INP, 'pi_inp', '', '.'),
    (XML_FILE_PATH_ES_CVM_IN_EXTR_DTOS_RIESG_FINPR_M_DATALAB_M, 'ES_CVM_IN_EXTR_DTOS_RIESG_FINPR_M_DATALAB_M',
     'cba8204c-0385-4c82-a748-f85b6f7adb56', INPUT_XML_PATH),
    (XML_FILE_PATH_ZERO_EURO_THRESHOLD_CROSSING, 'ZERO_EURO_THRESHOLD_CROSSING', '3fc8e5b7-be0c-489e-a62c-e3dcf601673b',
     INPUT_XML_PATH)]
test_update_config_id_data = [
    (XML_FILE_PATH_CKM_D_SIM_BDP_F_D, 'f1435656-6dd1-4413-b84c-ddb6c57cfaa5', 'f1435656-6dd1-4413-b84c-ddb6c57cfaa5'),
    (XML_FILE_PATH_DM_CAMPAIGN_BDP_D_D, '45685889-8139-2102-8395-156890482256', '12345-1234-1234-1234-1234567890'),
    (XML_FILE_PATH_PI_INP, '008b1d8e-697d-47dc-856e-5798ec6c9b50', '008b1d8e-697d-47dc-856e-5798ec6c9b50')]
test_prettify_data = [("<generalConfiguration><properties/></generalConfiguration>",
                       "<generalConfiguration>\n  <properties/>\n</generalConfiguration>\n")]


@pytest.mark.parametrize("mapping_file_path", test_read_mapping_file_mapping_data)
def test_read_mapping_file(mapping_file_path):
    config_df = XMLConverter().read_mapping(mapping_file_path)
    assert isinstance(config_df, pd.DataFrame)
    assert config_df.columns.tolist() == ["input_config_id_conversion", "input_config_id_ingestion",
                                          "output_config_id", "output_write_to_bq_dataset_name",
                                          "output_write_to_parquet_bucket_name", "interface"]
    assert config_df.size > 0


@pytest.mark.parametrize("conversion_path, mapping_file_path", test_read_conversion_xml_config_data)
def test_read_conversion_xml_config(conversion_path, mapping_file_path):
    config_list = XMLConverter().update_mapping_with_conversion_xml_path(conversion_path, mapping_file_path)
    assert len(config_list) > 0
    assert config_list[0].get('conversion_xml_path') is not None


@pytest.mark.parametrize("xml_path", test_read_conversion_xml_data)
def test_read_conversion_xml(xml_path):
    xml = XMLConverter.read_conversion_xml(xml_path)
    elements = XMLConverter.find_element(xml, 'dataConversions/dataConversion')
    assert len(elements) > 1


@pytest.mark.parametrize("xml_path", test_input_files)
def test_get_properties(xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    property_dict = XMLConverter.get_properties(data_conversion)
    assert isinstance(property_dict, dict)
    if 'ignore_trailing_columns' in property_dict.keys():
        assert len(list(property_dict.keys())) == 5
        assert {"datasource", 'country', 'interface', 'contains_header', 'ignore_trailing_columns'} == set(
            property_dict.keys())
    else:
        assert len(list(property_dict.keys())) == 4
        assert {"datasource", 'country', 'interface', 'contains_header'} == set(property_dict.keys())


@pytest.mark.parametrize("ingestion_path, config_id", test_read_ingestion_xml_data)
def test_read_ingestion_xml(ingestion_path, config_id):
    xml = XMLConverter.read_ingestion_xml(ingestion_path, config_id)
    data_flow = xml.find('dataFlows/dataFlow')
    assert len(data_flow) > 1
    assert data_flow.attrib['configurationId'] == config_id


@pytest.mark.parametrize("conversion_path, ingestion_path, output_path, mapping_file_path, data_source, table_name",
                         test_xml_converter_data)
def test_xml_converter(conversion_path, ingestion_path, output_path, mapping_file_path, data_source, table_name):
    output_file_path = os.path.join(output_path, data_source, table_name, constant.OUTPUT_XML_FILE_NAME)
    XMLConverter().xml_converter(conversion_path, ingestion_path, output_path, mapping_file_path)
    if table_name == 'INVALID':
        assert not os.path.isfile(output_file_path)
    else:
        expected_xml_path = os.path.join(EXPECTED_DIR, table_name, constant.OUTPUT_XML_FILE_NAME)
        assert os.path.isfile(output_file_path)
        diff = main.diff_files(output_file_path, expected_xml_path)
        assert len(diff) is 0


@pytest.mark.parametrize("xml_path, country, parquet_bucket_name", test_update_general_configuration_data)
def test_update_general_configuration(xml_path, country, parquet_bucket_name):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    XMLConverter.update_general_configuration(xml, country, parquet_bucket_name)
    assert xml.find('generalConfiguration/properties') is not None
    root_path = xml.find('generalConfiguration/properties/property[@name="rootPath"]')
    assert root_path.attrib['value'] == f'gs://vf{country}-dh-rawingested'
    gcs_output_root_path = xml.find('generalConfiguration/properties/property[@name="gcsOutputRootPath"]')
    assert gcs_output_root_path.attrib['value'] == f'gs://{parquet_bucket_name}'


@pytest.mark.parametrize("xml_path", test_input_files)
def test_update_root_data(xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    country = data_conversion.find('properties/property[@name="country"]')
    country = country.attrib['value']
    XMLConverter.update_data_root(data_conversion, country)
    root_data = data_conversion.find('dataRoot')
    assert root_data is not None
    assert root_data.attrib['path'] == "${rootPath}/${datasource}/${interface}/${version}"


@pytest.mark.parametrize("xml_path", test_input_files)
def test_update_field_actions(xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    XMLConverter.update_field_actions(data_conversion)
    field_actions = data_conversion.findall('fieldActions/fieldAction')
    assert field_actions is not None
    for field_action in field_actions:
        if field_action.attrib['actionParameter'] == 'anonymise':
            assert '${anon_version}' in field_action.attrib['actionMethod']
        elif field_action.attrib['actionParameter'] == 'replace' and re.search('email',
                                                                               field_action.attrib['actionArguments']):
            assert field_action.attrib['actionArguments'] == r'replacementValue=<email@replaced\>'
            assert field_action.attrib['actionMethod'] == "(?i)[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$" \
                                                          "%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*" \
                                                          "[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
        elif field_action.attrib['actionParameter'] == 'replace' and re.search('phone number',
                                                                               field_action.attrib['actionArguments']):
            assert field_action.attrib['actionArguments'] == r'replacementValue=<phone number>'

            assert field_action.attrib['actionMethod'] == r"\d{4,}"


@pytest.mark.parametrize("xml_path, dataset_name", test_update_conversion_actions_data)
def test_update_conversion_actions(xml_path, dataset_name):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    property_dict = XMLConverter.get_properties(data_conversion)
    XMLConverter().update_conversion_actions(data_conversion, property_dict)

    write_actions = data_conversion.findall('conversionActions/writeActions/')
    assert write_actions is not None

    actual_actions = []
    for write_action in write_actions:
        actual_actions.append(write_action.attrib['name'])

    expected_actions = ['WriteToBigQueryAction', 'WriteToParquetAction']
    assert expected_actions == actual_actions

    for write_action in write_actions:
        write_action_name = write_action.attrib['name']
        if write_action_name == 'WriteToBigQueryAction':
            dataset_element = write_action.find('config/dataset')
            assert dataset_element.attrib[
                       'name'] == "vf${country}_${bigQueryProject}_lake_${bigQueryDataCategory}_rawprepared_s"
        elif write_action_name == 'WriteToParquetAction':
            path_element = write_action.find('config/path')
            assert path_element.text == '${gcsOutputRootPath}/${interface}/${version}/parquet'

    write_action = data_conversion.find('conversionActions/writeAction')
    assert write_action is None

    read_action = data_conversion.find('conversionActions/readAction')
    assert read_action is not None
    assert read_action.attrib['name'] == 'ReadFromCsvAction' or 'ReadFromEncodedCsvAction'
    files = read_action.find('files')
    assert files is not None
    assert files.attrib['charset'] == 'utf-8' or 'iso-8859-1'
    assert files.attrib['contains_header'] == 'true' or 'false'


@pytest.mark.parametrize("xml_path, interface, input_config_id, ingestion_path", test_update_partitions_data)
def test_update_partitions(xml_path, interface, input_config_id, ingestion_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    XMLConverter().update_partitions(data_conversion, interface, input_config_id, ingestion_path)
    partitions = data_conversion.find('partitions')
    assert partitions is not None
    if input_config_id == 'cba8204c-0385-4c82-a748-f85b6f7adb56':
        assert partitions.find('partition[@name="fileRegex"]') is not None
        assert partitions.find('partition[@name="3"]') is not None
        assert partitions.find('partition[@name="4"]') is not None
    elif input_config_id == '3fc8e5b7-be0c-489e-a62c-e3dcf601673b':
        assert partitions.find('partition[@name="fileRegex"]') is not None
        assert partitions.find('partition[@name="2"]') is not None
        assert partitions.find('partition[@name="3"]') is not None
        assert partitions.find('partition[@name="4"]') is not None
    else:
        assert partitions.find('partition[@name="year"]') is not None
        assert partitions.find('partition[@name="month"]') is not None
        assert partitions.find('partition[@name="day"]') is not None


@pytest.mark.parametrize("xml_path, input_config_id_conversion, output_config_id", test_update_config_id_data)
def test_update_config_id(xml_path, input_config_id_conversion, output_config_id):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    XMLConverter.update_config_id(data_conversion, input_config_id_conversion, output_config_id)
    actual_config_id = data_conversion.attrib['configurationId']
    assert actual_config_id == output_config_id


@pytest.mark.parametrize("input_xml_string, expected_xml_string", test_prettify_data)
def test_prettify(input_xml_string, expected_xml_string):
    prettified_string = XMLConverter._XMLConverter__prettify(input_xml_string)
    assert expected_xml_string == prettified_string


@pytest.mark.parametrize('xml_path, dataset_name', test_update_conversion_properties_data)
def test_update_conversion_properties(xml_path, dataset_name):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    XMLConverter.update_conversion_properties(data_conversion, dataset_name)
    assert data_conversion.find("properties/property[@name='contains_header']") is None
    assert data_conversion.find("properties/property[@name='ignore_trailing_columns']") is None

    big_query_project = data_conversion.find("properties/property[@name='bigQueryProject']")
    big_query_data_category = data_conversion.find("properties/property[@name='bigQueryDataCategory']")

    assert big_query_project is not None
    assert big_query_data_category is not None

    dataset_name_split = dataset_name.split('_')

    assert big_query_project.attrib['value'] == dataset_name_split[1]
    assert big_query_data_category.attrib['value'] == dataset_name_split[3]


@pytest.mark.parametrize('xml_path', test_input_files)
def test_update_read_action(xml_path):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    conversion = data_conversion.find('conversionActions/readAction')
    files = conversion.find('files')
    assert files is not None
    files_start = files_end = None
    if 'type' in files.attrib and files.attrib['type'] == 'fixedWidth':
        field = files.find('field[5]')
        files_start = field.attrib['start']
        files_end = field.attrib['end']

    property_dict = XMLConverter.get_properties(data_conversion)
    XMLConverter.update_read_action(conversion, property_dict)

    if conversion.attrib['name'] == 'ReadFromEncodedCsvAction':
        assert files.attrib['contains_header'] == 'false'
        assert files.attrib['charset'] == 'iso-8859-1'
    else:
        assert files.attrib['contains_header'] == 'true'
        assert files.attrib['charset'] == 'utf-8'

    if 'type' in files.attrib and files.attrib['type'] == 'fixedWidth':
        assert files.find('field') is None
        schema_field = data_conversion.find('schema/field[5]')
        schema_start = schema_field.attrib['start']
        schema_end = schema_field.attrib['end']
        assert files_start == schema_start
        assert files_end == schema_end


@pytest.mark.parametrize('xml_path, table', test_update_write_actions_data)
def test_update_write_actions(xml_path, table):
    parser = element_tree.XMLParser(remove_blank_text=False)
    xml = element_tree.parse(xml_path, parser)
    data_conversion = xml.find('dataConversions/dataConversion')
    conversion_actions = XMLConverter.find_element(data_conversion, 'conversionActions')
    conversion_action = element_tree.SubElement(conversion_actions, 'writeActions')

    XMLConverter.update_write_actions(conversion_action, table)

    write_actions = conversion_action.findall('writeAction')
    assert len(write_actions) == 2
