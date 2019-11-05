from __future__ import absolute_import
import logging
from abc import ABC
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name-upd-stgclic-custscontact-to-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'


def printval(value):
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


class UnnestCoGrouped(beam.DoFn, ABC):
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
    Target_table_data_query = """SELECT
  CUSTOMER_KEY,
  PRIMARY_ADDRESS_KEY,
  BILLING_ADDRESS_KEY,
  CUSTOMER_ID,
  ETL_SOURCE_SYSTEM,
  SOURCE_CREATE_DT,
  PRIMARY_PHONE_NUMBER,
  DERIVED_AREA_CODE,
  BILLSHIP_ADDR_SYNC_IND,
  PROVIDIAN_ACCT_NBR,
  PROVIDIAN_MARKETING_CODE,
  PROVIDIAN_UPDATE_DT,
  PURGED_IND,
  PURGED_DT,
  MERGED_IND,
  MERGED_DT,
  MERGED_SOURCE_ACCT_ID,
  TEST_CUSTOMER_IND,
  BLOCK_CURRENT_IND,
  BLOCK_LIFETIME_IND,
  IDCENTRIC_INDIVIDUAL_ID,
  IDCENTRIC_HOUSEHOLD_ID,
  IDCENTRIC_ADDRESS_ID,
  VOID_IND,
  INS_BATCH_NBR,
  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
  MKTADDR_ADDRESS_KEY,
  FST_ORDER_DT,
  FST_ORDER_SUBSIDIARY_KEY,
  FST_ORDER_ITEM_KEY,
  FST_SHIP_DT,
  FST_SHIP_SUBSIDIARY_KEY,
  FST_SHIP_ITEM_KEY,
  TV_TO_DOTCOM_DT,
  DOTCOM_TO_TV_DT,
  SCND_SHIP_DT,
  SCND_SHIP_SUBSIDIARY_KEY,
  SCND_SHIP_ITEM_KEY,
  FST_SHIP_ORDER_ID,
  SCND_SHIP_ORDER_ID,
  PRIMARY_PHONE_TYPE,
  ALT_PHONE1_NUMBER,
  ALT_PHONE1_TYPE,
  ALT_PHONE2_NUMBER,
  ALT_PHONE2_TYPE,
  FST_ORDER_ORDER_ID,
  GUEST_CODE,
  DIGITAL_CUSTOMER_ID,
  MARKET_PLACE_ID,
  MARKET_PLACE_CUST_ID,
  FST_SHIPPED_SHIP_DT,
  PRIVACY_IND
FROM
  `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`"""

    Source_contact_data_query = """SELECT
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
          END AS DYN_MSG_PROMO_IND
FROM
  `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACT` b"""

    p = beam.Pipeline(options=options)
    primary_pipeline = 'target_table'
    target_table = p | 'Read From Customer Target Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=Target_table_data_query, use_standard_sql=True))

    join_pipeline = 'source_table'
    source_table = (p | 'Read From Contact Source Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=Source_contact_data_query, use_standard_sql=True)))

    common_key = 'CUSTOMER_ID'
    pipelines_dictionary = {primary_pipeline: target_table, join_pipeline: source_table}

    (pipelines_dictionary
        | 'Left join' >> LeftJoin(primary_pipeline, target_table, join_pipeline, source_table, common_key)
        | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                                                output_table,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
