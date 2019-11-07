from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=upd-stgclic-custscontacthist-to-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'


def printval(value):
    print('#######################')
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
    attribute_ref_table = """SELECT * FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`"""

    purged_data = """SELECT
      CAST(d.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
      d.ROW_CREATED_DATE AS PURGED_DT,
      '1' AS PURGED_IND
    FROM
      `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACTHIST` d
      WHERE RECORD_TYPE = 'Removed Block'
  """

    merged_data = """SELECT
      CAST(d.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
      CAST(d.HSN_ACCT_NUM AS INT64) AS MERGED_SOURCE_ACCT_ID,
      d.ROW_CREATED_DATE AS MERGED_DT,
      '1' AS MERGED_IND
    FROM
      `automatic-asset-253215.STAGE.STG_CLIC_CUSTSCONTACTHIST` d
      WHERE RECORD_TYPE = 'Merged Account'
      """

    p = beam.Pipeline(options=options)
    primary_pipeline = 'attribute_ref_table_data'
    attribute_ref_table_data = p | 'Read From Customer Target Table' >> beam.io.Read(beam.io.BigQuerySource(
        query=attribute_ref_table,use_standard_sql=True))

    purge_pipeline = 'purge_file'
    purge_file = p | 'Purge File' >> beam.io.Read(beam.io.BigQuerySource(
        query=purged_data,use_standard_sql=True))

    merge_pipeline = 'merge_file'
    merge_file = p | 'Merge File' >> beam.io.Read(beam.io.BigQuerySource(
        query=merged_data,use_standard_sql=True))

    common_key = 'CUSTOMER_ID'
    purge_pipelines_dictionary = {primary_pipeline:attribute_ref_table_data,purge_pipeline:purge_file}

    join_pipeline2 = 'purge_left_join'
    purge_left_join = (purge_pipelines_dictionary | 'Updating Purged Columns' >> LeftJoin(
        primary_pipeline,attribute_ref_table_data,purge_pipeline,purge_file,common_key))

    merge_pipelines_dictionary = {join_pipeline2:purge_left_join,merge_pipeline:merge_file}

    (merge_pipelines_dictionary | 'Updating Merged Columns' >> LeftJoin(
        join_pipeline2,purge_left_join,merge_pipeline,merge_file,common_key)
     | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
