########################################################################################################
#
#     Author          :     Vishal Paliwal
#     Description     :     This Script updates records into the IM_CUSTOMER_ADDRESS_SCD Table  
#                           from STG_CLIC_CUSTADDRORG Table.
#     Version         :     1.2
#     Created         :     13.11.2019
#     Modified        :     15.11.2019
#
#     P.S: This script points to a specific GCP environment, 
#          for running it in a different environment make suitable changes in some configuration fields. 
#########################################################################################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging
from apache_beam.pvalue import AsDict

dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=upd-stgclic-custaddrorg-to-imcustomeraddress-scd',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcp_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ADDRESS_SCD'
p = beam.Pipeline(options=options)


def printVal(value):
    print('##############')
    print(value)


class LeftJoin2(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self,src_pipeline,src_pipeline_name,join_pipeline,join_pipeline_name,common_key):
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


class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self,src_pipeline,src_pipeline_name,join_pipeline,join_pipeline_name,common_key):
        self.join_pipeline = join_pipeline
        self.src_pipeline_name = src_pipeline_name
        self.src_pipeline = src_pipeline
        self.join_pipeline_name = join_pipeline_name
        self.common_key = common_key

    def expand(self,pcolls):
        def _format_as_common_key_tuple(data_dict,common_key):
            return [data_dict[key] for key in common_key], data_dict

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


def lookup(row, cust_ids):
    result = row.copy()
    try:
        result.update(cust_ids[str(row['CUSTOMER_ID'])+row['ADDR_NAME']+row['ETL_SOURCE_SYSTEM']])
        return result
    except KeyError as InsertRecords:
        print('skip')


def filter_out_nones(row):
    if row is not None:
        yield row
    else:
        print('Filtering out the None values...')


def run():
    address_scd = """SELECT * FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD`"""

    upd_addrorg_data = """SELECT
  CAST(a.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
  a.ADDRESS_NAME AS ADDR_NAME,
  'CLIC' AS ETL_SOURCE_SYSTEM,
  a.FILE_SET_DATE AS ETL_END_EFFECTIVE_DT,
  '0' AS ETL_CURRENT_IND,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
FROM
  `automatic-asset-253215.STAGE.STG_CLIC_CUSTADDRORG` a"""

    primary_pipeline_1 = 'p1'
    p1 = p | 'AddressSCD Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=address_scd,
        use_standard_sql=True))

    join_pipeline_1 = 'j1'
    j1 = p | 'AddressORG Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=upd_addrorg_data,
        use_standard_sql=True))

    common_key = {'CUSTOMER_ID', 'ADDR_NAME', 'ETL_SOURCE_SYSTEM'}
    pipelines_dictionary_1 = {primary_pipeline_1: p1, join_pipeline_1: j1}

    p1j1 = (pipelines_dictionary_1 | 'Updating addrs Fields' >> LeftJoin(
        primary_pipeline_1, p1, join_pipeline_1, j1, common_key))

    ins_addrorg_data = """SELECT
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
  TIMESTAMP_ADD(a.FILE_SET_DATE, INTERVAL 1 DAY) AS ETL_BEGIN_EFFECTIVE_DT,
  CAST('2099-12-31 00:00:00' AS TIMESTAMP) AS ETL_END_EFFECTIVE_DT,
  '1' AS ETL_CURRENT_IND,
  '' AS ETL_VERSION_NBR,
  '0' AS VOID_IND,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S',
    CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S',
    CURRENT_DATETIME()) AS INT64) AS INS_BATCH_NBR
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

    lookup_data = (p | 'Read from addrorg' >> beam.io.Read(beam.io.BigQuerySource(
        query=ins_addrorg_data, use_standard_sql=True))
        | 'Get Cust_Ids ' >> beam.Map(
                lambda row: (str(row['CUSTOMER_ID'])+row['ADDR_NAME']+row['ETL_SOURCE_SYSTEM'], row))
                )

    primary_pipeline_2 = 'p2'
    p2 = (p1 | 'Filter current records' >> beam.Filter(
          lambda row: row['ETL_CURRENT_IND'] == 1)
          | 'Lookup' >> beam.Map(lookup, AsDict(lookup_data))
          | 'Filter' >> beam.ParDo(filter_out_nones)
          )

    join_pipeline_2 = 'j2'
    j2 = p | 'Read From Attribute Ref Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=Attribute_ref_query, use_standard_sql=True))

    common_key = 'CUSTOMER_ID'
    pipelines_dictionary_2 = {primary_pipeline_2: p2, join_pipeline_2: j2}

    primary_pipeline_3 = 'p2j2'
    p2j2 = (pipelines_dictionary_2
    | 'Left join' >> LeftJoin2(primary_pipeline_2, p2, join_pipeline_2, j2, common_key)
    | 'Filter Nulls' >> beam.Filter(filter_null))

    version_nbr_query = """SELECT
  CUSTOMER_ID,
  ADDR_NAME,
  ETL_SOURCE_SYSTEM,
  ETL_VERSION_NBR+1 AS ETL_VERSION_NBR
FROM
  `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD`
WHERE
  ETL_CURRENT_IND = 1"""

    join_pipeline_3 = 'j3'
    j3 =  p | 'Read for Version NBR' >> beam.io.Read(beam.io.BigQuerySource(
        query=version_nbr_query, use_standard_sql=True))

    common_key = {'CUSTOMER_ID', 'ADDR_NAME', 'ETL_SOURCE_SYSTEM'}
    pipelines_dictionary_3 = {primary_pipeline_3: p2j2, join_pipeline_3: j3}

    p3 = pipelines_dictionary_3 | 'Updating Version NBR' >> LeftJoin(primary_pipeline_3, p2j2, join_pipeline_3, j3, common_key)

    primary_pipeline_4 = 'p4'
    p4 = (p1j1, p3) | 'Merge PCollections' >> beam.Flatten()

    void_ind_query = """SELECT
  CUSTOMER_ADDRESS_KEY,
  CUSTOMER_KEY,
  CUSTOMER_ID,
  ADDR_NAME,
  ETL_SOURCE_SYSTEM,
  CASE
    WHEN ETL_BEGIN_EFFECTIVE_DT > ETL_END_EFFECTIVE_DT THEN 1
  ELSE
  0
END
  AS VOID_IND
FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD`"""

    join_pipeline_4 = 'j4'
    j4 = p | 'Read for Void_Ind' >> beam.io.Read(beam.io.BigQuerySource(
        query=void_ind_query, use_standard_sql=True))

    common_key = {'CUSTOMER_ADDRESS_KEY', 'CUSTOMER_KEY', 'CUSTOMER_ID', 'ADDR_NAME', 'ETL_SOURCE_SYSTEM'}
    pipelines_dictionary_4 = {primary_pipeline_4: p4, join_pipeline_4: j4}

    (pipelines_dictionary_4 | 'Updating Void Ind Field' >> LeftJoin(primary_pipeline_4, p4, join_pipeline_4, j4, common_key)
                            | 'Write to IM_CUSTOMER_ADDRESS_SCD' >> beam.io.WriteToBigQuery(
                                       output_table,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                       create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
