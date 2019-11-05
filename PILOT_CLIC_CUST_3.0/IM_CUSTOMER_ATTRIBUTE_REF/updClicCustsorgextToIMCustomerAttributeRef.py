from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=xfm-vustgclic-custsorgext-to-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'direct'


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
    source_orgext_data_query = """SELECT
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
              a.Primary_phone_type AS Primary_phone_type,
              a.Alt_phone1_number AS  Alt_phone1_number,
              a.Alt_phone1_type AS Alt_phone1_type,
              a.Alt_phone2_number AS Alt_phone2_number,
              a.Alt_phone2_type AS Alt_phone2_type,
              CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
    FROM STAGE.STG_CLIC_CUSTSORGEXT a"""

    target_table_data_query = """
        SELECT
      CUSTOMER_KEY,
      PRIMARY_ADDRESS_KEY,
      BILLING_ADDRESS_KEY,
      CUSTOMER_ID,
      ETL_SOURCE_SYSTEM,
      CUSTOMER_SINCE_DT,
      FIRST_NAME,
      LAST_NAME,
      CLIC_EMAIL_SOURCE_CODE,
      CLIC_EMAIL_ADDRESS,
      CLIC_EMAIL_LAST_UPDT,
      DOTCOM_EMAIL_ADDRESS,
      DOTCOM_EMAIL_LAST_UPDT,
      MEDIA_CODE,
      EMPLOYEE_ID,
      SUPPRESS_EMAIL_IND,
      SEND_NEWS_IND,
      SUPPRESS_CALL_IND,
      SUPPRESS_MAIL_IND,
      BANK_DIRECT_DEBIT_IND,
      HAS_FLEXPAY_LIMIT_IND,
      NATIONAL_TAX_EXEMPT_IND,
      FLEXPAY_IND,
      ALLOW_ONAIR_IND,
      ALLOW_REFUND_IND,
      SHIP_ADDR_IND,
      STATE_TAX_EXEMPT_IND,
      AUTOSHIP_CUSTOMER_IND,
      ABUSIVE_RETURNS_IND,
      ABUSIVE_RETURNS_CNT,
      CALL_TAG_CNT,
      EMPTY_BOX_CNT,
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
      MKTADDR_ADDRESS_KEY,
      DYN_MSG_PROMO_IND,
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
      FST_ORDER_ORDER_ID,
      FST_SHIPPED_SHIP_DT,
      PRIVACY_IND
    FROM
      `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`
      """

    p = beam.Pipeline(options=options)
    primary_pipeline = 'target_table'
    target_table = p | 'Read From Customer Target Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=target_table_data_query,
        use_standard_sql=True))

    join_pipeline = 'source_table'
    source_table = (p | 'Read From Contact Source Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=source_orgext_data_query,use_standard_sql=True))
                    )

    common_key = 'CUSTOMER_ID'
    pipelines_dictionary = {primary_pipeline:target_table,join_pipeline:source_table}

    (pipelines_dictionary
     | 'Left join' >> LeftJoin(primary_pipeline,target_table,join_pipeline,source_table,common_key)
     | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
