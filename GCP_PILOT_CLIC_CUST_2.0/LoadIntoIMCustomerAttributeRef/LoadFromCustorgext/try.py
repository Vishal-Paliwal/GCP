import apache_beam as beam
#from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import logging
from apache_beam.pvalue import AsDict

my_options = {'--project=automatic-asset-253215',
              '--job_name=xfm-vustgclic-custsorgext-to-imcustomerattributeref',
              '--temp_location=gs://raw_source_files/Customers/temp',
              '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(my_options)
gcp_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'direct'

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
srg_table = 'automatic-asset-253215:STAGE.STG_CLIC_SURROGKEYS'
p = beam.Pipeline(options=options)
#client = bigquery.Client()


def printVal(value):
    print(value)


def lookup(row, cust_ids):
    result = row.copy()
    try:
        result.update(cust_ids[row['CUSTOMER_ID']])
        #updateRecords(result)
    except KeyError as UnMatchedRecords:
        return result


def filter_out_nones(row):
    if row is not None:
        yield row
    else:
        print('we found a none! get it out')


#def updateSrgKey():
#    count_query = """SELECT MAX(CUSTOMER_ID) FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`"""
#    query_job = client.query(count_query)
#
#    for row in query_job:
#        upd_query = """UPDATE `automatic-asset-253215.STAGE.STG_CLIC_SURROGKEYS`
#                       SET MAX_VALUE_KEY = row
#                       WHERE TABLE_NAME = "IM_CUSTOMER_ATTRIBUTE_REF"
#                       """
#        client.query(upd_query)


def run():
    main_query = """SELECT
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
          (srg_key.MAX_VALUE_KEY + ROW_NUMBER() OVER()) AS CUSTOMER_KEY,
          '999999999999' AS PRIMARY_ADDRESS_KEY,
          '999999999999' AS BILLING_ADDRESS_KEY,
          'CLIC' AS ETL_SOURCE_SYSTEM,
          '0' AS PURGED_IND,
          '0' AS MERGED_IND,
          '0' AS TEST_CUSTOMER_IND,
          '0' AS BLOCK_CURRENT_IND,
          '0' AS BLOCK_LIFETIME_IND, 
          '999999999999' AS IDCENTRIC_INDIVIDUAL_ID,
          '999999999999' AS IDCENTRIC_HOUSEHOLD_ID,
          '999999999999' AS IDCENTRIC_ADDRESS_ID,
          '0' AS VOID_IND,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS INS_BATCH_NBR,
          '999999999999' AS MKTADDR_ADDRESS_KEY,
          '0' AS PRIVACY_IND
        FROM
          `automatic-asset-253215.STAGE.STG_CLIC_CUSTSORGEXT` a,
          `automatic-asset-253215.STAGE.STG_CLIC_SURROGKEYS` srg_key
          WHERE srg_key.TABLE_NAME = "IM_CUSTOMER_ATTRIBUTE_REF"
          """

    lookup_query = """SELECT DISTINCT CUSTOMER_ID FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`"""

    IM_data = (p | 'Read Cust_Ids From IM' >> beam.io.Read(beam.io.BigQuerySource(
        query=lookup_query, use_standard_sql=True))
               | 'Get Cust_Ids ' >> beam.Map(lambda row:(row['CUSTOMER_ID'],row))
               )

    cust_org_data = (p | 'Read from custorgext' >> beam.io.Read(beam.io.BigQuerySource(
                                                        query=main_query, use_standard_sql=True))
                     | 'Lookup' >> beam.Map(lookup, AsDict(IM_data))
                     | 'Filter' >> beam.ParDo(filter_out_nones)
                     | 'Insert: Unmatched Records' >> beam.io.WriteToBigQuery(
                                                        output_table,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
                     #| 'Updating Surrogate_Key' >> beam.ParDo(updateSrgKey)
                     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
