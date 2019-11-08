########################################################################################################
#
#     Author          :     Vishal Paliwal
#     Description     :     This Script updates various Fields in the IM_CUSTOMER_ATTRIBUTE_REF Table from 
#                           STG_CLIC_CUSTSORGEXT, STG_CLIC_CUSTCONTACT and STG_CLIC_CUSTCONTACTHIST Table.
#     Version         :     1.0
#     Created         :     05.11.2019
#     Modified        :     08.11.2019
#
#     P.S: This script points to a specific GCP environment, 
#          for running it in a different environment make suitable changes in some configuration fields. 
#########################################################################################################

from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=upd-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'


def printval(value):
    print('#################')
    print(value)


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


def run():
    p = beam.Pipeline(options=options)

    customer_attrbute_ref_data = """SELECT * FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`"""
    primary_pipeline_1 = 'target_table'
    target_table = p | 'Target Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=customer_attrbute_ref_data,
        use_standard_sql=True))

    common_key = 'CUSTOMER_ID'

# ------- UPDATING IM_CUSTOMER_ATTRIBUTE_REF TABLE FROM STG_CLIC_CUSTSORGEXT TABLE ----------

    custsorgext_data = """SELECT
              CAST(a.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
              CAST(a.ROW_CREATED_DATE AS TIMESTAMP) AS SOURCE_CREATE_DT,
              a.PRIMARY_PHONE_NUM AS PRIMARY_PHONE_NUMBER,
              CAST(SUBSTR(a.PRIMARY_PHONE_NUM,1,3) AS INT64) AS DERIVED_AREA_CODE,
              CASE WHEN LENGTH(a.BILL_SHIP_ADDR_SYNC_FLAG) = 0 THEN 0
              ELSE CAST(a.BILL_SHIP_ADDR_SYNC_FLAG AS INT64) 
              END AS BILLSHIP_ADDR_SYNC_IND,
              a.GUEST_CUSTOMER_FLAG AS GUEST_CODE,
              a.GUID AS DIGITAL_CUSTOMER_ID,
              a.MARKET_PLACE_ID AS MARKET_PLACE_ID,
              a.MARKET_PLACE_CUSTID AS MARKET_PLACE_CUST_ID,
              a.Primary_phone_type AS PRIMARY_PHONE_TYPE,
              a.Alt_phone1_number AS  ALT_PHONE1_NUMBER,
              a.Alt_phone1_type AS ALT_PHONE1_TYPE,
              a.Alt_phone2_number AS ALT_PHONE2_NUMBER,
              a.Alt_phone2_type AS ALT_PHONE2_TYPE,
              CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
    FROM STAGE.STG_CLIC_CUSTSORGEXT a"""

    join_pipeline_1 = 'custsorgext_table'
    custsorgext_table = p | 'Custsorgext Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=custsorgext_data,use_standard_sql=True))

    pipelines_dictionary_1 = {primary_pipeline_1:target_table,join_pipeline_1:custsorgext_table}

    primary_pipeline_2 = 'updated_data_1'
    updated_data_1 = pipelines_dictionary_1 | 'Updating Custsorgext Fields' >> LeftJoin(
        primary_pipeline_1,target_table,join_pipeline_1,custsorgext_table,common_key)

# ------- UPDATING IM_CUSTOMER_ATTRIBUTE_REF TABLE FROM STG_CLIC_CUSTCONTACT TABLE ----------

    contact_data = """SELECT
      CAST(b.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
      b.CUSTOMER_SINCE_DATE AS CUSTOMER_SINCE_DT,
      b.FIRST_NAME AS FIRST_NAME,
      b.LAST_NAME AS LAST_NAME,
      b.EMAIL_SOURCE_CODE AS CLIC_EMAIL_SOURCE_CODE,
      b.EMAIL_ADDRESS AS CLIC_EMAIL_ADDRESS,
      b.LAST_EMAIL_DATE AS CLIC_EMAIL_LAST_UPDT,
      b.MEDIA_TYPE AS MEDIA_CODE,
      b.HSN_EMP_ID AS EMPLOYEE_ID,
      CASE WHEN b.SUPRESS_EMAIL_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS  SUPPRESS_EMAIL_IND,
      CASE WHEN b.SEND_NEWS_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS SEND_NEWS_IND,
      CASE WHEN b.SUPRESS_CALL_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS SUPPRESS_CALL_IND,
      CASE WHEN b.SUPRESS_MAIL_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS SUPPRESS_MAIL_IND,
      CASE WHEN b.DIRECT_DEBIT_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS BANK_DIRECT_DEBIT_IND,
      CASE WHEN b.FLEX_PAY_LIMIT_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS HAS_FLEXPAY_LIMIT_IND,
      CASE WHEN b.NAT_TAX_EXEMPT_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS NATIONAL_TAX_EXEMPT_IND,
      CASE WHEN b.NO_FLEXPAY_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS FLEXPAY_IND,
      CASE WHEN b.NO_ON_AIR_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS ALLOW_ONAIR_IND,
      CASE WHEN b.NO_REFUND_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS ALLOW_REFUND_IND,
      CASE WHEN b.NO_SHIP_ADDR_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS SHIP_ADDR_IND,
      CASE WHEN b.ST_TAX_EXEMPT_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS STATE_TAX_EXEMPT_IND,
      CASE WHEN b.AUTOSHIP_CUST_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS AUTOSHIP_CUSTOMER_IND,
      CASE WHEN b.ABUSIVE_RETURNS_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS ABUSIVE_RETURNS_IND,
      CAST(b.ABUSIVE_RETURNS_COUNT AS INT64) AS ABUSIVE_RETURNS_CNT,
      CAST(b.CALL_TAG_COUNT AS INT64) AS CALL_TAG_CNT,
      CAST(b.EMPTY_BOX_COUNT AS INT64) AS EMPTY_BOX_CNT,
      CASE WHEN b.PROMO_FLAG = 'Y' THEN 1
              ELSE 0 
              END AS DYN_MSG_PROMO_IND,
      CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
    FROM
      `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACT` b"""

    join_pipeline_2 = 'contact_table'
    contact_table = p | 'Custcontact Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=contact_data, use_standard_sql=True))

    pipelines_dictionary_2 = {primary_pipeline_2: updated_data_1, join_pipeline_2: contact_table}

    primary_pipeline_3 = 'updated_data_2'
    updated_data_2 = pipelines_dictionary_2 | 'Updating Contact Fields' >> LeftJoin(
        primary_pipeline_2, updated_data_1, join_pipeline_2, contact_table, common_key)

# ------- UPDATING IM_CUSTOMER_ATTRIBUTE_REF TABLE FROM STG_CLIC_CUSTCONTACTHIST TABLE ----------

    purged_data = """SELECT
          CAST(d.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
          d.ROW_CREATED_DATE AS PURGED_DT,
          '1' AS PURGED_IND,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
        FROM
          `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACTHIST` d
          WHERE RECORD_TYPE = 'Removed Block'
      """

    merged_data = """SELECT
      CAST(d.DELETED_HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
      CAST(d.HSN_ACCT_NUM AS INT64) AS MERGED_SOURCE_ACCT_ID,
      d.ROW_CREATED_DATE AS MERGED_DT,
      '1' AS MERGED_IND,
      CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
    FROM
      `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACTHIST` d
      WHERE RECORD_TYPE = 'Merged Account'
      """

    purge_pipeline = 'purge_file'
    purge_file = p | 'Purge File' >> beam.io.Read(beam.io.BigQuerySource(
        query=purged_data,use_standard_sql=True))

    merge_pipeline = 'merge_file'
    merge_file = p | 'Merge File' >> beam.io.Read(beam.io.BigQuerySource(
        query=merged_data,use_standard_sql=True))

    purge_pipelines_dictionary = {primary_pipeline_3:updated_data_2,purge_pipeline:purge_file}

    join_pipeline_3 = 'purge_left_join'
    purge_left_join = (purge_pipelines_dictionary | 'Updating Purged Fields' >> LeftJoin(
        primary_pipeline_3,updated_data_2,purge_pipeline,purge_file,common_key))

    merge_pipelines_dictionary = {join_pipeline_3:purge_left_join,merge_pipeline:merge_file}

    (merge_pipelines_dictionary | 'Updating Merged Fields' >> LeftJoin(
        join_pipeline_3,purge_left_join,merge_pipeline,merge_file,common_key)
     | 'Update IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

