import logging
import os
import re
from io import StringIO

import lxml.etree as element_tree
import pandas as pd

from dif2bif import constant

logger = logging.getLogger()
logging.basicConfig(format='%(levelname)s:%(message)s', level=os.environ.get("LOG_LEVEL", "INFO"))


class XMLConverter:
    @staticmethod
    def read_mapping(mapping_file_path):
        logger.info(f"Reading `{mapping_file_path}`")
        return pd.read_csv(mapping_file_path)

    def update_mapping_with_conversion_xml_path(self, conversion_path, mapping_file_path):
        input_config_df = self.read_mapping(mapping_file_path)
        logger.info('Creating conversion config')
        parser = element_tree.XMLParser(remove_blank_text=False)
        config_from_xml = list()
        for path, subdir, file_names in os.walk(conversion_path):
            for filename in file_names:
                if filename.endswith('_conversion.xml'):
                    xml_path = os.path.join(path, filename)
                    try:
                        xml = element_tree.parse(xml_path, parser)
                        data_conversion = XMLConverter.find_element(xml, 'dataConversions/dataConversion')
                        config_id_in_xml = data_conversion.attrib['configurationId']
                    except Exception as ex:
                        logger.error(f'Error occurred in reading xml `{xml_path}` due to {ex}')
                        continue
                    config_from_xml.append((config_id_in_xml, xml_path))

        if config_from_xml:
            column_names = ['input_config_id_conversion', 'conversion_xml_path']
            config_from_xml_df = pd.DataFrame(data=config_from_xml, columns=column_names)
            config_with_xml_path_df = input_config_df.merge(config_from_xml_df, on=column_names[:1])
            config_with_xml_path = config_with_xml_path_df.where((pd.notnull(config_with_xml_path_df)),
                                                                 None).to_dict(orient='records')
            if not config_with_xml_path:
                raise Exception(
                    'No records to process. Please check input mapping file that has correct value for interface and '
                    'input conversion config id.')
            return config_with_xml_path
        else:
            raise Exception('No records to process. Please check input config.')

    @staticmethod
    def read_conversion_xml(xml_path):
        logger.info(f'Reading conversion xml `{xml_path}`')
        parser = element_tree.XMLParser(remove_blank_text=False)
        return element_tree.parse(xml_path, parser)

    def xml_converter(self, conversion_path, ingestion_path, output_path, mapping_file_path):
        config_including_xml_path = self.update_mapping_with_conversion_xml_path(conversion_path=conversion_path,
                                                                                 mapping_file_path=mapping_file_path)
        for config in config_including_xml_path:
            try:
                converted_xml = self.modify_conversion_xml(config, ingestion_path)
                data_conversion = XMLConverter.find_element(converted_xml, 'dataConversions/dataConversion')
                properties_dict = self.get_properties(data_conversion)
                output_full_path = os.path.join(output_path, properties_dict['datasource'],
                                                properties_dict['interface'])

                if not os.path.exists(output_full_path):
                    os.makedirs(output_full_path)
                output_file_path = os.path.join(output_full_path, constant.OUTPUT_XML_FILE_NAME)

                xml_str = element_tree.tostring(converted_xml, pretty_print=True).decode("utf-8")
                prettified_xml_str = XMLConverter.__prettify(xml_str)
                XMLConverter.__output_to_file(output_file_path, prettified_xml_str)

                logger.info(f"Written converted xml output to '{output_file_path}'")
            except Exception as ex:
                logger.error(f"Unable to convert `{config['conversion_xml_path']}` due to `{ex}`")

    @staticmethod
    def __output_to_file(output_file_path, prettified_xml_str):
        with open(output_file_path, "w") as f:
            for line in prettified_xml_str.split('\n'):
                if re.search(r'actionMethod="(?i)', line):
                    line = line.replace('\'', '&apos;')
                f.write(line + '\n')

    @staticmethod
    def __prettify(xml_text):
        parser = element_tree.XMLParser(remove_blank_text=True)
        string_io = StringIO(xml_text)
        tree = element_tree.parse(string_io, parser)
        return element_tree.tostring(tree, pretty_print=True).decode('utf-8')

    @staticmethod
    def update_conversion_properties(data_conversion, dataset_name):
        properties = XMLConverter.find_element(data_conversion, 'properties')
        _property = XMLConverter.find_element(properties, 'property[@name="contains_header"]')
        if _property is not None:
            properties.remove(_property)

        _property = XMLConverter.find_element(properties, 'property[@name="ignore_trailing_columns"]')
        if _property is not None:
            properties.remove(_property)

        big_query_project = re.search('(vf[a-zA-Z]*_)(\\w+)_lake', dataset_name).group(2)
        big_query_data_category = re.search('(vf[a-zA-Z0-9_]*_lake_)(\\w+)_rawprepared_s', dataset_name).group(2)

        element_tree.SubElement(properties, 'property', name='bigQueryProject', value=big_query_project)
        element_tree.SubElement(properties, 'property', name='bigQueryDataCategory', value=big_query_data_category)

    @staticmethod
    def get_properties(data_conversion):
        property_dict = dict()
        _properties = XMLConverter.findall_elements(data_conversion, "properties/")
        for _property in _properties:
            name = _property.attrib['name']
            if name in ('interface', 'datasource', 'contains_header', 'country', 'ignore_trailing_columns'):
                property_dict[name] = _property.attrib['value']
        return property_dict

    @staticmethod
    def update_config_id(data_conversion, input_config_id_conversion, output_config_id):
        if input_config_id_conversion != output_config_id:
            data_conversion.attrib['configurationId'] = data_conversion.attrib['configurationId'].replace(
                input_config_id_conversion, output_config_id)

    @staticmethod
    def update_config(config, properties):
        if config['interface'] is None:
            interface = XMLConverter.find_element(properties, 'property[@name="interface"]')
            interface = interface.attrib['value']
            config['interface'] = interface

        if config['input_config_id_ingestion'] is None:
            config['input_config_id_ingestion'] = config['input_config_id_conversion']

        if config['output_config_id'] is None:
            config['output_config_id'] = config['input_config_id_conversion']

        if config['output_write_to_bq_dataset_name'] is None or config['output_write_to_parquet_bucket_name'] is None:
            datasource = XMLConverter.find_element(properties, 'property[@name="datasource"]')
            datasource = datasource.attrib['value']

            country = XMLConverter.find_element(properties, 'property[@name="country"]')
            country = country.attrib['value']

            if config['output_write_to_bq_dataset_name'] is None:
                config['output_write_to_bq_dataset_name'] = f'vf{country}_dh_lake_{datasource}_rawprepared_s'

            if config['output_write_to_parquet_bucket_name'] is None:
                config['output_write_to_parquet_bucket_name'] = f'vf{country}-dh-{datasource}-rawprepared'

        return config

    def modify_conversion_xml(self, config, ingestion_path):
        xml = self.read_conversion_xml(config['conversion_xml_path'])
        data_conversion = XMLConverter.find_element(xml, 'dataConversions/dataConversion')
        config = self.update_config(config, XMLConverter.find_element(data_conversion, 'properties'))

        try:
            property_dict = self.get_properties(data_conversion)
            self.update_config_id(data_conversion, config['input_config_id_conversion'], config['output_config_id'])
            self.update_data_root(data_conversion, property_dict['country'])
            self.update_partitions(data_conversion, property_dict['interface'], config['input_config_id_ingestion'],
                                   ingestion_path)
            self.update_field_actions(data_conversion)
            self.update_general_configuration(xml, property_dict['country'],
                                              config['output_write_to_parquet_bucket_name'])
            self.update_conversion_actions(data_conversion, property_dict)
            self.update_conversion_properties(data_conversion, config['output_write_to_bq_dataset_name'])
        except Exception as ex:
            raise Exception(
                f"Unable to convert XML with id {config['input_config_id_conversion']} due to {ex}")
        return xml

    @staticmethod
    def update_field_actions(data_conversion):
        field_actions = XMLConverter.find_element(data_conversion, 'fieldActions')
        field_action_elements = XMLConverter.findall_elements(data_conversion, 'fieldActions/')
        for field_action in field_action_elements:
            if field_action.attrib['actionParameter'] == 'anonymise':
                field_action.attrib['actionMethod'] = field_action.attrib['actionMethod'][:8] \
                                                      + '${anon_version}' + field_action.attrib['actionMethod'][10:]
            elif field_action.attrib['actionParameter'] == 'replace' and \
                    field_action.attrib['actionMethod'] == 'email,phone':
                field_action.attrib['actionMethod'] = "(?i)[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$" \
                                                      "%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*" \
                                                      "[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
                field_action.attrib['actionArguments'] = r"replacementValue=<email@replaced\>"

                element_tree.SubElement(field_actions, 'fieldAction',
                                        attrib={"fieldName": field_action.attrib['fieldName'],
                                                "actionMethod": r"\d{4,}",
                                                "actionType": field_action.attrib['actionType'],
                                                "actionParameter": field_action.attrib['actionParameter'],
                                                "actionArguments": r"replacementValue=<phone number>"})

    @staticmethod
    def update_data_root(data_conversion, country):
        replacement = {'hdfsRootPath': 'rootPath', f'/vf_{country}': ''}
        pattern = re.compile("|".join(replacement.keys()))
        data_root = XMLConverter.find_element(data_conversion, 'dataRoot')
        data_root.attrib['path'] = pattern.sub(lambda m: replacement[m.group(0)], data_root.attrib['path'])
        data_root.attrib['path'] = data_root.attrib['path'].replace("/vf_${country}", '')

    @staticmethod
    def update_general_configuration(xml, country, parquet_bucket_name):
        properties_element = XMLConverter.find_element(xml, 'generalConfiguration/properties')
        properties = XMLConverter.findall_elements(properties_element, 'property')
        for _property in properties:
            properties_element.remove(_property)
        element_tree.SubElement(properties_element, 'property', name='rootPath',
                                value=f'gs://vf{country}-dh-rawingested')
        element_tree.SubElement(properties_element, 'property', name='gcsOutputRootPath',
                                value=f'gs://{parquet_bucket_name}')

    @staticmethod
    def __update_partition_fileregex(conversion_partitions, ingestion_partitions, parameters):
        for partition in XMLConverter.findall_elements(conversion_partitions, 'partition'):
            if 'source' in partition.attrib and 'filepath' in partition.attrib['source']:
                conversion_partitions.remove(partition)

        for parameter in XMLConverter.findall_elements(parameters, 'parameter'):
            if parameter.attrib['name'] == 'fileRegex':
                element_tree.SubElement(conversion_partitions, 'partition',
                                        name=parameter.attrib['name'],
                                        source="property",
                                        position=parameter.attrib['value']
                                        )
            else:
                value = parameter.attrib['value']
                partition = XMLConverter.find_element(ingestion_partitions,
                                                      f'partition[@name="{value}"]')
                if partition is not None:
                    element_tree.SubElement(conversion_partitions, 'partition',
                                            name=parameter.attrib['name'],
                                            source=partition.attrib['source'],
                                            position=partition.attrib['position']
                                            )

    @staticmethod
    def __update_partition_position(interface, conversion_partitions):
        table_name_length = len(interface)
        for partition in XMLConverter.findall_elements(conversion_partitions, 'partition'):
            if 'filepath' in partition.attrib['source']:
                partition.attrib['source'] = partition.attrib['source'].replace('filepath', 'filename')
                if partition.attrib['name'] == 'year':
                    partition.attrib['position'] = str(table_name_length + constant.YEAR_START_INDEX) + ":" + \
                                                   str(table_name_length + constant.MONTH_START_INDEX)
                if partition.attrib['name'] == 'month':
                    partition.attrib['position'] = str(table_name_length + constant.MONTH_START_INDEX) + ":" + \
                                                   str(table_name_length + constant.DAY_START_INDEX)
                if partition.attrib['name'] == 'day':
                    partition.attrib['position'] = str(table_name_length + constant.DAY_START_INDEX) + ":" + \
                                                   str(table_name_length + constant.HOUR_START_INDEX)
                if partition.attrib['name'] == 'hour':
                    partition.attrib['position'] = str(table_name_length + constant.HOUR_START_INDEX) + ":" + \
                                                   str(table_name_length + constant.HOUR_END_INDEX)
            elif 'filename' in partition.attrib['source']:
                partition.attrib['source'] = re.sub('filename:(year|month|day|hour)', 'filename',
                                                    partition.attrib['source'])
                if 'fieldType' in partition.attrib:
                    partition.attrib.pop("fieldType")

    @staticmethod
    def __update_partition_datetime(conversion_partitions):
        for partition in XMLConverter.findall_elements(conversion_partitions, 'partition'):
            if partition.attrib['source'] == 'datetime':
                partition.attrib['source'] = "datetime_now"
                if 'fieldType' in partition.attrib:
                    partition.attrib.pop("fieldType")
                if 'position' in partition.attrib:
                    partition.attrib.pop("position")

                name = partition.attrib['name']
                property_position = XMLConverter.find_element(conversion_partitions, f'partition[@position="{name}"]')
                if name and property_position is not None and name == property_position.attrib['position']:
                    conversion_partitions.remove(property_position)

        def sort_by_attributes(element):
            if element.attrib["name"] == 'fileRegex':
                return 0
            elif element.attrib["name"] == 'year' or (
                    'position' in element.attrib and element.attrib["position"] == 'year'):
                return 1
            elif element.attrib["name"] == 'month' or (
                    'position' in element.attrib and element.attrib["position"] == 'month'):
                return 2
            elif element.attrib["name"] == 'day' or (
                    'position' in element.attrib and element.attrib["position"] == 'day'):
                return 3
            elif element.attrib["name"] == 'hour' or (
                    'position' in element.attrib and element.attrib["position"] == 'hour'):
                return 4
            else:
                return 5

        partitions = XMLConverter.findall_elements(conversion_partitions, 'partition')
        conversion_partitions[:] = sorted(partitions, key=sort_by_attributes)

    def update_partitions(self, data_conversion, interface, input_config_id_ingestion, ingestion_path):
        conversion_partitions = XMLConverter.find_element(data_conversion, 'partitions')
        ingestion_xml = self.read_ingestion_xml(ingestion_path, input_config_id_ingestion)

        if conversion_partitions is not None:
            file_path = XMLConverter.find_element(conversion_partitions, 'partition[@source="filepath:year"]')
            datetime = XMLConverter.find_element(conversion_partitions, 'partition[@source="datetime"]')
            if file_path is not None and ingestion_xml:
                data_flow = XMLConverter.find_element(ingestion_xml, 'dataFlows/dataFlow')
                ingestion_partitions = XMLConverter.find_element(data_flow, 'to/partitions')
                parameters = XMLConverter.find_element(data_flow, 'preIngestionTasks/task/parameters')
                if parameters is not None and ingestion_partitions is not None:
                    file_regex_parameter = XMLConverter.find_element(parameters, 'parameter[@name="fileRegex"]')
                    if file_regex_parameter is not None:
                        XMLConverter.__update_partition_fileregex(conversion_partitions, ingestion_partitions,
                                                                  parameters)
                    else:
                        XMLConverter.__update_partition_position(interface, conversion_partitions)
                else:
                    XMLConverter.__update_partition_position(interface, conversion_partitions)
            elif file_path is not None:
                XMLConverter.__update_partition_position(interface, conversion_partitions)
            else:
                XMLConverter.__update_partition_position(interface, conversion_partitions)

            if datetime is not None:
                XMLConverter.__update_partition_datetime(conversion_partitions)
        else:
            if ingestion_xml:
                ingestion_partitions = XMLConverter.find_element(ingestion_xml, 'dataFlows/dataFlow/to/partitions')
                if len(ingestion_partitions) > 0:
                    data_conversion.append(ingestion_partitions)
                else:
                    raise Exception(f'Unable to get partitions')
            else:
                raise Exception(f'Unable to read ingestion file')

    @staticmethod
    def update_read_action(conversion_action, properties_dict):
        data_conversion = conversion_action.getparent().getparent()
        files = XMLConverter.find_element(conversion_action, 'files')
        files.set('contains_header', properties_dict['contains_header'])
        if 'ignore_trailing_columns' in properties_dict.keys():
            files.set('ignore_trailing_columns', properties_dict['ignore_trailing_columns'])

        if conversion_action.attrib['name'] == 'ReadFromEncodedCsvAction':
            files.set('charset', 'iso-8859-1')
        else:
            files.set('charset', 'utf-8')

        files_field = XMLConverter.findall_elements(files, 'field')
        if files_field and files.attrib.get('type') == 'fixedWidth':
            schema = XMLConverter.find_element(data_conversion, 'schema')
            if schema is not None:
                schema_field = XMLConverter.findall_elements(schema, 'field')
                if len(files_field) == len(schema_field):
                    for f_field, s_field in zip(files_field, schema_field):
                        s_field.set('start', f_field.attrib['start'])
                        if 'end' in f_field.attrib:
                            s_field.set('end', f_field.attrib['end'])
                        files.remove(f_field)
                else:
                    logger.warning(
                        'Number of records in files and schema fields are not matching so not setting start/end '
                        'attributes in schema fields')

    @staticmethod
    def update_write_actions(conversion_action, table):
        #  WriteToBigQueryAction
        write_action = element_tree.SubElement(conversion_action, 'writeAction', name='WriteToBigQueryAction')
        config = element_tree.SubElement(write_action, 'config')
        element_tree.SubElement(config, 'dataset',
                                name="vf${country}_${bigQueryProject}_lake_${bigQueryDataCategory}_rawprepared_s")
        element_tree.SubElement(config, 'tableName', name=table)

        #  WriteToParquetAction
        write_action = element_tree.SubElement(conversion_action, 'writeAction', name='WriteToParquetAction')
        config = element_tree.SubElement(write_action, 'config')
        path = element_tree.SubElement(config, 'path')
        path.text = "${gcsOutputRootPath}/${interface}/${version}/parquet"

    def update_conversion_actions(self, data_conversion, properties_dict):
        conversion_actions = XMLConverter.find_element(data_conversion, 'conversionActions')
        element_tree.SubElement(conversion_actions, 'writeActions')
        conversion_actions = XMLConverter.findall_elements(data_conversion, 'conversionActions/')

        for conversion_action in conversion_actions:
            if conversion_action.tag == 'readAction':
                self.update_read_action(conversion_action, properties_dict)
            elif conversion_action.tag == 'writeActions':
                table = properties_dict['interface'].lower()
                table = re.sub('[^a-zA-Z0-9 \n.]', '_', table)
                self.update_write_actions(conversion_action, table)
            elif conversion_action.tag == 'writeAction':
                parent = conversion_action.getparent()
                parent.remove(conversion_action)

    @staticmethod
    def read_ingestion_xml(ingestion_path, ingestion_conf_id):
        for path, subdir, file_names in os.walk(ingestion_path):
            for file_name in file_names:
                if file_name.endswith('_ingestion.xml'):
                    xml_path = os.path.join(path, file_name)
                    try:
                        xml = element_tree.parse(xml_path)
                        data_flow_element = XMLConverter.find_element(xml, 'dataFlows/dataFlow')
                        ingestion_conf_id_in_xml = data_flow_element.attrib['configurationId']
                        if ingestion_conf_id == ingestion_conf_id_in_xml:
                            logger.info(f'Found ingestion xml `{xml_path}`')
                            return xml
                    except Exception as ex:
                        logger.error(f'Error occurred in reading ingestion file `{xml_path}` due to {ex}')

    @staticmethod
    def find_element(root, xpath):
        return root.find(xpath)

    @staticmethod
    def findall_elements(root, xpath):
        return root.findall(xpath)
