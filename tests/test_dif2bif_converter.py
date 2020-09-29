import os
from pathlib import Path

import lxml.etree as etree
import pytest

from dif2bif.dif2bif_converter import main

ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_XML_FILE_NAME = 'configuration_conversion.xml'

test_conversion_data = [(f'{ROOT_DIR}/tests/resources/input_xmls',
                         f'',
                         f'{ROOT_DIR}/target/test_output',
                         f'{ROOT_DIR}/tests/resources/mapping.csv', 'ckm', 'DM_CAMPAIGN_BDP_D_D',
                         '45685889-8139-2102-8395-156890482256'),

                        (f'{ROOT_DIR}/tests/resources/input_xmls',
                         f'{ROOT_DIR}/tests/resources/input_xmls',
                         f'{ROOT_DIR}/target/test_output',
                         f'{ROOT_DIR}/tests/resources/mapping.csv', 'bss', 'CKM_D_SIM_BDP_F_D',
                         'f1435656-6dd1-4413-b84c-ddb6c57cfaa5')
                        ]


@pytest.mark.parametrize("conversion_path, ingestion_path, output, mapping_file_path, datasource, interface, config_id",
                         test_conversion_data)
def test_conversion(conversion_path, ingestion_path, output, mapping_file_path, datasource, interface, config_id):
    xml_path = os.path.join(output, datasource, interface, OUTPUT_XML_FILE_NAME)

    if os.path.isfile(xml_path):
        os.remove(xml_path)

    main(conversion_path, ingestion_path, output, mapping_file_path)
    xml = etree.parse(xml_path)
    c_element = xml.find('.//dataConversion')
    partition_fields = xml.findall('.//partition')
    conf_id_in_xml = c_element.attrib['configurationId']
    assert conf_id_in_xml == config_id
    assert partition_fields is not None
