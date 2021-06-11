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

"""Google Cloud function code to extract periodic transactions from data source."""

import base64
import json
import logging
import os
import sys

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import bigquery
import google.cloud.logging

from custom_functions import hook_get_load_predictions_query
from custom_functions import hook_get_bq_schema
from custom_functions import hook_apply_formulas
from custom_functions import hook_on_completion

# Set-up logging
logger = logging.getLogger('predict_transactions_batch')
logger.setLevel(logging.DEBUG)
handler = None
if os.getenv('LOCAL_LOGGING'):
  handler = logging.StreamHandler(sys.stderr)
else:
  client = google.cloud.logging.Client()
  handler = google.cloud.logging.handlers.CloudLoggingHandler(client)
logger.addHandler(handler)

BQ_LTV_GCP_PROJECT = str(os.getenv("BQ_LTV_GCP_PROJECT", ""))
BQ_LTV_DATASET = str(os.getenv("BQ_LTV_DATASET", ""))
BQ_LTV_PREDICTIONS_TABLE = str(
    os.getenv("BQ_LTV_PREDICTIONS_TABLE", ""))


def _load_data_from_bq(query):
  """Loads all the transactions from the table.

  Args:
    query: A string with the query to run on the table

  Returns:
    A dataframe with all the table data
  """
  job_config = bigquery.job.QueryJobConfig()

  return bigquery.Client(project=BQ_LTV_GCP_PROJECT).query(query, job_config=job_config).to_dataframe()


def _write_to_bigquery(df, table_name):
  """Writes the given dataframe into the BQ table.

    Args:
      df: A pandas dataframe representing the data to be written
      table_name: A string representing the full path of the metadata BQ table
  """

  dataframe = df

  client = bigquery.Client(project=BQ_LTV_GCP_PROJECT)

  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_TRUNCATE"
  job_config.schema = hook_get_bq_schema()

  job = client.load_table_from_dataframe(
      dataframe, table_name, job_config=job_config)
  job.result()

  table = client.get_table(table_name)
  print("Loaded {} rows and {} columns to {}".format(table.num_rows,
                                                     len(table.schema),
                                                     table_name))

def _delete_dataset(dataset):
  """Deletes the dataset specified by the dataset parameter.
    Args:
      dataset:  The name of the dataset to be deleted.
  """
  client = bigquery.Client()
  client.delete_dataset(
      dataset, delete_contents=True, not_found_ok=True
  )

def main(event: Dict[str, Any], context=Optional[Context]):
  """Checks if the data source table is available & no extract table generated.

    Depending on the existence it will trigger the data transfer.

    Args:
      event (dict):  The dictionary with data specific to this type of event.
        The `data` field contains the PubsubMessage message. The `attributes`
        field will contain custom attributes if there are any.
      context (google.cloud.functions.Context): The Cloud Functions event
        metadata. The `event_id` field contains the Pub/Sub message ID. The
        `timestamp` field contains the publish time.
  """
  del context

  data = base64.b64decode(event["data"]).decode("utf-8")
  msg = json.loads(data)

  input_dataset = (msg['operation']
      ['outputInfo']['bigqueryOutputDataset']).split("://")[1]
  input_table = f"""{input_dataset}.predictions_*"""

  output_table = f'{BQ_LTV_GCP_PROJECT}.{BQ_LTV_DATASET}.{BQ_LTV_PREDICTIONS_TABLE}_{msg["date"]}'

  query = hook_get_load_predictions_query(input_table)
  _write_to_bigquery(
      hook_apply_formulas(_load_data_from_bq(query)), output_table)
  _delete_dataset(input_dataset)
  hook_on_completion()


def _test():
  msg_data = base64.b64encode(bytes('{"payload": {"bq_input_to_predict_table": "decent-fulcrum-316414.test.filtered_periodic_transactions", "bq_output_table": "decent-fulcrum-316414.test.predictions", "date": "20210401", "operation": {"name": "projects/988912752389/locations/europe-west4/batchPredictionJobs/7138777155428679680", "displayName": "pablogil_test_pltv_batch_predict - 2021-06-17 13:27:04.054958", "model": "projects/988912752389/locations/europe-west4/models/7662206262901211136", "inputConfig": {"instancesFormat": "bigquery", "bigquerySource": {"inputUri": "bq://decent-fulcrum-316414.test.filtered_periodic_transactions_20210401"}}, "outputConfig": {"predictionsFormat": "bigquery", "bigqueryDestination": {"outputUri": "bq://decent-fulcrum-316414"}}, "dedicatedResources": {"machineSpec": {"machineType": "n1-highmem-8"}, "startingReplicaCount": 20, "maxReplicaCount": 20}, "manualBatchTuningParameters": {"batchSize": 100}, "outputInfo": {"bigqueryOutputDataset": "bq://decent-fulcrum-316414.prediction_automl_training_data_20200605_0608_2021_06_17T06_27_04_428Z"}, "state": "JOB_STATE_SUCCEEDED", "completionStats": {"successfulCount": "280"}, "createTime": "2021-06-17T13:27:04.571081Z", "startTime": "2021-06-17T13:27:05.550439Z", "endTime": "2021-06-17T13:44:29Z", "updateTime": "2021-06-17T13:45:41.481342Z"}}, "status_check_url": "https://europe-west4-aiplatform.googleapis.com/v1/projects/988912752389/locations/europe-west4/batchPredictionJobs/7138777155428679680", "success_topic": "pablogil_test.pltv.post_process_batch_predictions", "concurrent_slot_document": "pablogil_test_pltv_prediction_tracking/concurrent_document", "status_success_values": ["JOB_STATE_SUCCEEDED"], "status_error_values": ["JOB_STATE_FAILED", "JOB_STATE_EXPIRED"], "inserted_timestamp": "0", "error_topic": "pablogil_test.pltv.", "expiration_timestamp": "0", "status_field": "state", "updated_timestamp": "0", "source_topic": "pablogil_test.pltv.predict_transactions_batch"}'.encode("utf-8")))

  main(
      event={
          "data": msg_data,
          "attributes": {
              "forwarded": "true"
          }
      },
      context=None)


if __name__ == "__main__":
  _test()
