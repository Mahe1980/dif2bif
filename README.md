# dif2bif

`dif2bif` folder contains the below utilities.
`tests` folder contains unit tests for all the utilities.

|script|description|
|------|-----------|
|dif2bif_converter.py|Convert DSS to BIF compliant XMLs|
|create_bigquery_schema.py|Create BigQuery schema files using the converted XMLs|
|create_bigquery_table.py|Create BigQuery tables using schema files created|

### Setting up the virtual environment

1. Update `PYTHONPATH` to include current project (Applicable only if you are running on Windows machine)

       export PYTHONPATH=${PYTHONPATH}:<project root directory>

2. Ensure that Python version 3.x.x is installed on the system.

3. Add the following environment variables to let python use the proxy (it's recommended to add these to your `.bash_profile`):

        export http_proxy=http://vfukukproxy.internal.vodafone.com:8080/proxy.pac
        export https_proxy=http://vfukukproxy.internal.vodafone.com:8080/proxy.pac
        export no_proxy=*.local,169.254/16,*.internal.vodafone.com,127.0.0.1,localhost

4. Create a new virtual env specific to this project e.g.

        python -m venv  .venv
    
   where `.venv` is the folder the virtual environment is configured in.  `.venv` is only an example, it can be a path as well.

5. Activate the virtual environment e.g.
    
        source .venv/bin/activate

6. Navigate to project root directory and run the below command to install necessary python modules

        pip install -r requirements.txt


## Test
The tests will use the XML(conversion, ingestion) and config files available in `tests\resources` folder.

1) From the project root directory, run `python -m pytest tests` using command line

2) Converted XML and Schema files will be created in `target\test_output\` folder.


## Steps:


 **XML Converter:**


1) Clone the repository which contains BDP XMLs.
  
      e.g. https://gitlab.rat.bdp.vodafone.com/bda-it/bda-it-dss (for Italy)

2) Create `mapping.csv` (any location, e.g. <root_path_of_this_repo>\resource\) with entries for XMLs that need to be converted.
  Here is an example `mapping.csv`.
    
    |input_config_id_conversion|input_config_id_ingestion|output_config_id|output_write_to_bq_dataset_name|output_write_to_parquet_bucket_name|interface|
    |-------------------------|--------------------------|----------------|-------------------------------|-----------------------------------|---------|
    |f1435656-6dd1-4413-b84c-ddb6c57cfaa5|f1435656-6dd1-4413-b84c-ddb6c57cfaa5|f1435656-6dd1-4413-b84c-ddb6c57cfaa5|vfit_dh_lake_ckm_rawprepared_s|vfit-dh-ckm-rawprepared|CKM_D_SIM_BDP_F_D|
    |008b1d8e-697d-47dc-856e-5798ec6c9b50||||||
  
    input_config_id_conversion: It is mandatory. This is the conversion input config id.
    input_config_id_ingestion: It will be optional. If not provided, the script will use the input conversion configuration id to look up the configuration_ingestion.xml file.
    output_config_id: It will be optional. If not provided the script will use the input config id conversion in the output XML created.
    output_write_to_bq_dataset_name: It will be optional. If not provided, the script will compose it using country and datasource properties defined in XML.
    output_write_to_parquet_bucket_name: It will be optional. If not provided, the script will compose it using country and datasource properties defined in XML.
    interface: It will be optional. If not provided, the script will use the value read from the input conversion XML.

    The details of each column is available in the page:
  
    https://confluence.sp.vodafone.com/display/BDPL/Automation+of+XML+Conversion+-+DSS+To+BIF
      
3) Navigate to `dif2bif` folder and use the below command to run the converter tool:

       python dif2bif_converter.py --conversion_path <absolute conversion xml path> --ingestion_path <absolute ingestion xml path> --output_path <absolute output path> --mapping_file_path <absolute file path to mapping.csv>
                   
4) Converted XMLs will be available in the output location as below:
    
    {output_location}/{data_source}/{interface}/configuration_conversion.xml

                               

**Create BQ Table:**


1) Create `config.yaml` (any location, e.g. <root_path_of_this_repo>\resource\) to hold project_id and table name to relative path to converted XML that need to be created in BigQuery
    e.g

        `project_id: vf-it-ca-nonlive
        table_to_relative_config_path: [{'CKM_D_SIM_BDP_F_D':"tests/new_output/bss/CKM_D_SIM_BDP_F_D"},
                                        {'DM_CAMPAIGN_BDP_D_D': "tests/new_output/ckm/DM_CAMPAIGN_BDP_D_D"},
                                       ]`
  
2) Run the below command to create schema.
     
       python create_bigquery_schema.py --base_path <absolute base folder path for converted xmls> --config_file_path <absolute path for file config.yaml>
     
   Note: base path is input to this script and relative path is in `config.yaml` to form a full path
   
   This will create schema files with the name `<table>_schema.json` in the same location    
     
3) Once schema files are generated, run the below command to create Big Query table.
    
       python create_bigquery_table.py --base_path <absolute base folder path for converted xmls> --config_file_path <absolute path for file config.yaml>
       
   Note: This script will not delete dataset or table. It only creates if it doesn't exist in the project.
   
   Alternatively bq table can be created using below command to create a partitioned table based on the column "partion_year_month_day"
  
       bq mk --table --schema configuration/conversion/<table>_schema.json --time_partitioning_field=partition_year_month_day <project-id>:<dataset>.<table_name>
   
  Note: Partitioning field can be changed according to the requirement but the partitioning field name should be available in the schema file generated


  
**while running XML validator use below datastore.properties:**


**NON-LIVE**
project_id=vf-${opco}-ca-nonlive

namespace=build-chain (change this according to your requirement)

processing_group_kind=processing_group

feed_kind=feeds

schema_kind=schema

default_doc_name=default

rootPath=gs://vf-${opco}-ca-nonlive-qa/data/raw (replace ${opco} with code for lm)

outputrootPath=gs://vf-{opco}-ca-nonlive/   (replace ${opco} with code for lm)

anon_version=test

parquet_version =1.0 (change according to requirement)

**LIVE**

project_id=vf-{opco}-ca-live

namespace=<namespace for live>

processing_group_kind=processing_group

feed_kind=feeds

schema_kind=schema

default_doc_name=default

rootPath=gs://<datahub bucket>/data/raw

outputrootPath=gs://

anon_version={opco}

parquet_version =1.0 (change according to the requirement)
