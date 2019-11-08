########################################################################################################
#
#     Author          :     Vishal Paliwal
#     Description     :     This Script updates Customer Address Fields in the 
#                           IM_CUSTOMER_ATTRIBUTE_REF_TABLE from IM_CUSTOMER_ADDRESS_SCD Table.
#     Version         :     1.0
#     Created         :     08.11.2019
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

    def process(self,input_element,src_pipeline,join_pipeline):
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

    # ------- UPDATING IM_CUSTOMER_ATTRIBUTE_REF TABLE FROM STG_CLIC_CUSTSORGEXT TABLE ----------

    SHIPADDR0_addressSCD_data = """SELECT
              CUSTOMER_ADDRESS_KEY AS BILLING_ADDRESS_KEY,
              CUSTOMER_ADDRESS_KEY AS PRIMARY_ADDRESS_KEY,
              CUSTOMER_KEY,
              CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
    FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD` a
    WHERE a.ADDR_NAME = 'SHIPADDR0'"""

    BILLADDR1_addressSCD_data = """SELECT
                  CUSTOMER_ADDRESS_KEY AS BILLING_ADDRESS_KEY,
                  CUSTOMER_KEY,
                  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
        FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD` a
        WHERE a.ADDR_NAME = 'BILLADDR1'"""

    MKTADDR_addressSCD_data = """SELECT
                  CUSTOMER_ADDRESS_KEY AS MKTADDR_ADDRESS_KEY,
                  CUSTOMER_KEY,
                  CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR
        FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ADDRESS_SCD` a
        WHERE a.ADDR_NAME = 'MKTADDR'"""

    join_pipeline_SHIPADDR0 = 'SHIPADDR0_ADDRS'
    SHIPADDR0_ADDRS = p | 'SHIPADDR0_ADDRS' >> beam.io.Read(beam.io.BigQuerySource(
        query=SHIPADDR0_addressSCD_data,use_standard_sql=True))

    join_pipeline_BILLADDR1 = 'BILLADDR1_ADDRS'
    BILLADDR1_ADDRS = p | 'BILLADDR1_ADDRS' >> beam.io.Read(beam.io.BigQuerySource(
        query=BILLADDR1_addressSCD_data,use_standard_sql=True))

    join_pipeline_MKTADDR = 'MKTADDR_ADDRS'
    MKTADDR_ADDRS = p | 'MKTADDR_ADDRS' >> beam.io.Read(beam.io.BigQuerySource(
        query=MKTADDR_addressSCD_data,use_standard_sql=True))

    common_key_2 = 'CUSTOMER_KEY'

    pipelines_dictionary_1 = {primary_pipeline_1:target_table,join_pipeline_SHIPADDR0:SHIPADDR0_ADDRS}
    primary_pipeline_2 = 'SHIPADDR0_data'
    SHIPADDR0_data = (pipelines_dictionary_1 | 'Left Join 1' >> LeftJoin(
        primary_pipeline_1,target_table,join_pipeline_SHIPADDR0,SHIPADDR0_ADDRS,common_key_2)
                      )

    pipelines_dictionary_2 = {primary_pipeline_2:SHIPADDR0_data,join_pipeline_BILLADDR1:BILLADDR1_ADDRS}
    primary_pipeline_3 = 'BILLADDR1_data'
    BILLADDR1_data = (pipelines_dictionary_2 | 'Left Join 2' >> LeftJoin(
        primary_pipeline_2,SHIPADDR0_data,join_pipeline_BILLADDR1,BILLADDR1_ADDRS,common_key_2)
                      )

    pipelines_dictionary_3 = {primary_pipeline_3:BILLADDR1_data,join_pipeline_MKTADDR:MKTADDR_ADDRS}
    (pipelines_dictionary_3 | 'Left Join 3' >> LeftJoin(
        primary_pipeline_3,BILLADDR1_data,join_pipeline_MKTADDR,MKTADDR_ADDRS,common_key_2)
     | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
