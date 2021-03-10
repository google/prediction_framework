"""Google Cloud function code to extract periodic transactions from data source."""

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-

import base64
import json
import os

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import bigquery

from custom_functions import hook_get_load_predictions_query
from custom_functions import hook_get_bq_schema
from custom_functions import hook_apply_formulas



BQ_LTV_GCP_PROJECT = str(os.getenv('BQ_LTV_GCP_PROJECT', ''))

BQ_LTV_DATASET = str(os.getenv('BQ_LTV_DATASET', ''))


def _write_to_bigquery(df, table_name):
  """Writes the given dataframe into the BQ table.

  Args:
    df: A pandas dataframe representing the data to be written
    table_name: A string representing the full path of the metadata BQ table
  """

  dataframe = df

  client = bigquery.Client()

  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = 'WRITE_TRUNCATE'
  job_config.schema = hook_get_bq_schema()

  job = client.load_table_from_dataframe(
      dataframe, table_name, job_config=job_config)  # Make an API request.
  job.result()  # Wait for the job to complete.

  table = client.get_table(table_name)  # Make an API request.
  print('Loaded {} rows and {} columns to {}'.format(table.num_rows,
                                                     len(table.schema),
                                                     table_name))


def _delete_table(table_path):
  """Deletes the BQ table.

  Args:
    table_path: A string representing the full path of the BQ table
  """
  try:
    table_arr = table_path.split('.')
    project, dataset, table = table_arr
    client = bigquery.Client()
    query = f"""
      if exists(SELECT size_bytes FROM `{project}.{dataset}.__TABLES__`
        where  table_id = "{table}") then
         drop table `{project}.{dataset}.{table}`;
      END IF;"""

    client.query(query)  # Make an API request.
  # pylint: disable=bare-except
  except:
    print('Error happened while deleting the table {}'.format(table_path))
  # pylint: enable=bare-except


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Checks if the data source table is available & no extract table generated.

  Depending on the existence it will trigger the data transfer.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context

  data = base64.b64decode(event['data']).decode('utf-8')
  msg = json.loads(data)

  output_dataset = (msg['operation']['metadata']
      ['batch_predict_details']['output_info']['bigquery_output_dataset'])
  output_table = f'{BQ_LTV_GCP_PROJECT}.{BQ_LTV_DATASET}.{BQ_LTV_PREDICTIONS_TABLE}_{msg['date']}'

  _delete_table(output_table)
  df = hook_apply_formulas(
    hook_get_load_predictions_query(input_table), output_table)
  _write_to_bigquery(df, output_table)


  # date = msg['date']

  # client = bigquery.client.Client(project=DEFAULT_GCP_PROJECT)
  # job = client.copy_table(f'{output_dataset}.predictions',
  #     f'{BQ_LTV_GCP_PROJECT}.{BQ_LTV_DATASET}.predictions_{date}')
  # job.result()

def _test():
  message = {'date': '20210303', 'bq_input_to_predict_table': 'ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions', 'bq_output_table': 'ltv-framework', 'operation': {'name': 'projects/988912752389/locations/eu/operations/TBL8979557532418703360', 'metadata': OrderedDict([('@type', 'type.googleapis.com/google.cloud.automl.v1beta1.OperationMetadata'), ('createTime', '2021-03-05T17:57:54.251058Z'), ('updateTime', '2021-03-05T18:02:43.797899Z'), ('batchPredictDetails', {'inputConfig': {'bigquerySource': {'inputUri': 'bq://ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions_20210303'}}, 'outputInfo': {'bigqueryOutputDataset': 'bq://ltv-framework.prediction_training_data_20200605_0608_2021_03_05T09_57_54_169Z'}})]), 'done': True, 'response': OrderedDict([('@type', 'type.googleapis.com/google.cloud.automl.v1beta1.BatchPredictResult')])}}, 'expiration_timestamp': DatetimeWithNanoseconds(2021, 3, 5, 19, 36, 48, 201235, tzinfo=<UTC>), 'client_class': 'AutoMlClient', 'success_topic': 'pltv_fwk.jaimemm_tests.copy_batch_predictions', 'updated_timestamp': DatetimeWithNanoseconds(2021, 3, 5, 19, 36, 48, 201236, tzinfo=<UTC>), 'source_topic': 'pltv_fwk.jaimemm_tests.predict_transactions_batch', 'client_class_module': 'google.cloud.automl_v1beta1', 'error_topic': '', 'operation_name': 'projects/988912752389/locations/eu/operations/TBL8979557532418703360', 'inserted_timestamp': DatetimeWithNanoseconds(2021, 3, 5, 19, 36, 48, 201231, tzinfo=<UTC>), 'client_params': {'client_options': {'api_endpoint': 'eu-automl.googleapis.com:443'}}
  msg_data = base64.b64encode(bytes(json.dumps(message).encode('utf-8')))
  main(
      event={
          'data': msg_data,
          'attributes': {
              'forwarded': 'true'
          }
      },
      context=None)


  if __name__ == '__main__':
    _test()
