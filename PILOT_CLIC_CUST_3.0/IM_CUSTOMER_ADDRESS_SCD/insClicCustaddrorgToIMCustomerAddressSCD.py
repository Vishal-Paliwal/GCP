########################################################################################################
#
#     Author          :     Vishal Paliwal
#     Description     :     This Script inserts records into the IM_CUSTOMER_ADDRESS_SCD Table  
#                           from STG_CLIC_CUSTADDRORG Table.
#     Version         :     1.0
#     Created         :     05.11.2019
#     Modified        :     05.11.2019
#
#     P.S: This script points to a specific GCP environment, 
#          for running it in a different environment make suitable changes in some configuration fields. 
#########################################################################################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging

dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=ins-stgclic-custaddrorg-to-imcustomeraddress-scd',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcp_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ADDRESS_SCD'
p = beam.Pipeline(options=options)


def printVal(value):
    print(value)


class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self,src_pipeline,src_pipeline_name,join_pipeline,join_pipeline_name,common_key):
        super().__init__()
        self.join_pipeline = join_pipeline
        self.src_pipeline_name = src_pipeline_name
        self.src_pipeline = src_pipeline
        self.join_pipeline_name = join_pipeline_name
        self.common_key = common_key

    def expand(self,pcolls):
        def _format_as_common_key_tuple(data_dict,common_key):
            return data_dict[common_key],data_dict

        return ({pipeline_name:pcoll
                               | 'Convert to ({0}, object) for {1}'.format(self.common_key,pipeline_name) >> beam.Map(
            _format_as_common_key_tuple,self.common_key)
                 for (pipeline_name,pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),self.src_pipeline,
                                                   self.join_pipeline)
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests and emits the CogroupBykey output"""

    def process(self, input_element, src_pipeline, join_pipeline):
        group_key,grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline]
        source_dictionaries = grouped_dict[src_pipeline]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


def filter_null(element):
    if element['CUSTOMER_KEY'] != '':
        return element


def run():
    address_scd_query = """SELECT
  (srg_key.MAX_VALUE_KEY + ROW_NUMBER() OVER()) AS CUSTOMER_ADDRESS_KEY,
  '' AS CUSTOMER_KEY,
  CAST(a.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
  a.ADDRESS_NAME AS ADDR_NAME,
  'CLIC' AS ETL_SOURCE_SYSTEM,
  CAST(a.ROW_CREATED_DATE AS TIMESTAMP) AS SOURCE_CREATE_DT,
  a.ADDRESS_LINE_1 AS ADDR_LINE1_TXT,
  a.ADDRESS_LINE_2 AS ADDR_LINE2_TXT,
  a.CITY AS CITY_NAME,
  a.STATE AS STATE_CODE,
  a.COUNTRY AS COUNTRY_CODE,
  SUBSTR(a.ZIP_CODE,1,5) AS POSTAL_ZIP,
  SUBSTR(a.ZIP_CODE,6,9) AS POSTAL_ZIP4,
  CASE WHEN a.DISABLE_CLEANSING_FLAG = 'N' THEN 1
          ELSE 0 
          END AS ADDR_CLEANSING_IND,
  CASE WHEN a.FRAUD_BAD_ACCT_FLAG = 'Y' THEN 1
          ELSE 0 
          END AS ADDR_FRAUD_IND,
  CASE WHEN a.AGENT_VERIFIED_ADDRESS = 'Y' THEN 1
          ELSE 0 
          END AS ADDR_QAS_VERIFIED_IND,
  a.ADDRESS_TYPE_CODE AS ADDR_TYPE_CODE,
  a.SHIP_TO_FIRST_NAME AS SHIPTO_FIRST_NAME,
  a.SHIP_TO_LAST_NAME AS SHIPTO_LAST_NAME,
  CAST('1990-01-01 00:00:00' AS TIMESTAMP) AS ETL_BEGIN_EFFECTIVE_DT,
  CAST('2099-12-31 00:00:00' AS TIMESTAMP) AS ETL_END_EFFECTIVE_DT,
  '1' AS ETL_CURRENT_IND,
  '1' AS ETL_VERSION_NBR,
  '0' AS VOID_IND,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S',
    CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S',
    CURRENT_DATETIME()) AS INT64) AS INS_BATCH_NBR,
  '0' AS Privacy_Ind
FROM
  `automatic-asset-253215.STAGE.STG_CLIC_CUSTADDRORG` a,
  `automatic-asset-253215.STAGE.STG_CLIC_SURROGKEYS` srg_key
          WHERE srg_key.TABLE_NAME = "IM_CUSTOMER_ADDRESS_SCD"
   """

    Attribute_ref_query = """SELECT
    CUSTOMER_KEY,
    CUSTOMER_ID
  FROM
    `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF` b"""

    primary_pipeline = 'address_table'
    address_table = p | 'Read From addrorg table' >> beam.io.Read(beam.io.BigQuerySource(
        query=address_scd_query,
        use_standard_sql=True))

    join_pipeline = 'attribute_ref_table'
    attribute_ref_table = (p | 'Read From Attribute Ref Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=Attribute_ref_query,use_standard_sql=True))
                    )

    common_key = 'CUSTOMER_ID'
    pipelines_dictionary = {primary_pipeline:address_table,join_pipeline:attribute_ref_table}

    (pipelines_dictionary
     | 'Left join' >> LeftJoin(primary_pipeline,address_table,join_pipeline,attribute_ref_table,common_key)
     | 'Filter Nulls' >> beam.Filter(filter_null)
     | 'Insert Records' >> beam.io.WriteToBigQuery(
                       output_table,
                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                       create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
