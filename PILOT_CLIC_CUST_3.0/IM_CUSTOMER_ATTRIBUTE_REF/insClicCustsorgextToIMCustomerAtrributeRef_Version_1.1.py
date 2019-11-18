########################################################################################################
#
#     Author          :     Vishal Paliwal
#     Description     :     This Script initially inserts records into the IM_CUSTOMER_ATTRIBUTE_REF_GCP Table  
#                           from STG_CLIC_CUSTSORGEXT Table.
#     Version         :     1.1
#     Created         :     21.10.2019
#     Modified        :     18.11.2019
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

dataflow_options = {'--project=hsni-dev-edw',
                    '--job_name=ins-stgclic-custsorgext-to-imcustomerattributeref',
                    '--temp_location=gs://gcp_us_dev_edw_hsn_market_bucket/DataflowRunner/BigQueryWriteTemp',
                    '--staging_location=gs://gcp_us_dev_edw_hsn_market_bucket/Source_File/20190131',
                    '--region=us-east1',
                    '--subnetwork=https://www.googleapis.com/compute/v1/projects/network-qrg/regions/us-east1/subnetworks/gcp-non-prod-dataproc'}
options = PipelineOptions(dataflow_options)
gcp_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'

output_table = 'hsni-dev-edw:EDW_STG_US.IM_CUSTOMER_ATTRIBUTE_REF_GCP'
p = beam.Pipeline(options=options)


def printVal(value):
    print('###########')
    print(value)


def lookup(row,cust_ids):
    result = row.copy()
    try:
        result.update(cust_ids[row['CUSTOMER_ID']])
    except KeyError as InsertRecords:
        return result


def filter_out_nones(row):
    if row is not None:
        yield row
    else:
        print('Filtering out the None values...')


def run():
    source_custsorgext_query = """SELECT
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
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS INS_BATCH_NBR,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
          '999999999999' AS MKTADDR_ADDRESS_KEY,
          '0' AS PRIVACY_IND
        FROM
          `hsni-dev-edw.EDW_STG_US.STG_CLIC_CUSTSORGEXT` a,
          `hsni-dev-edw.EDW_STG_US.STG_CLIC_SURROGKEYS` srg_key
          WHERE srg_key.TABLE_NAME = "IM_CUSTOMER_ATTRIBUTE_REF_GCP"
          """

    lookup_query = """
    SELECT DISTINCT CUSTOMER_ID 
    FROM `hsni-dev-edw.EDW_STG_US.IM_CUSTOMER_ATTRIBUTE_REF_GCP`"""

    lookup_data = (p | 'Read Cust_Ids From target table' >> beam.io.Read(beam.io.BigQuerySource(
        query=lookup_query,use_standard_sql=True))
                   | 'Get Cust_Ids ' >> beam.Map(
                lambda row:(row['CUSTOMER_ID'],row))
                   )

    (p | 'Read from custorgext' >> beam.io.Read(beam.io.BigQuerySource(
        query=source_custsorgext_query,use_standard_sql=True))
     | 'Lookup' >> beam.Map(lookup,AsDict(lookup_data))
     | 'Filter' >> beam.ParDo(filter_out_nones)
     | 'Insert Records' >> beam.io.WriteToBigQuery(
                       output_table,
                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                       create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
