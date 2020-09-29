import yaml


def load_config(config_file_path):
    with open(config_file_path) as config_file:
        config = yaml.safe_load(config_file)
        project_id = config['project_id']
        table_relative_path_list = config['table_to_relative_config_path']
        return project_id, table_relative_path_list
