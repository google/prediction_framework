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
from custom_functions import hook_on_completion

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

  return bigquery.Client().query(query, job_config=job_config).to_dataframe()


def _write_to_bigquery(df, table_name):
  """Writes the given dataframe into the BQ table.

    Args:
      df: A pandas dataframe representing the data to be written
      table_name: A string representing the full path of the metadata BQ table
  """

  dataframe = df

  client = bigquery.Client()

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

  input_table = f"""{(msg['operation']['metadata']
                   ['batchPredictDetails']['outputInfo']['bigqueryOutputDataset']).split("://")[1]}.predictions"""

  output_table = f"{BQ_LTV_GCP_PROJECT}.{BQ_LTV_DATASET}.{BQ_LTV_PREDICTIONS_TABLE}_{msg['date']}"

  query = hook_get_load_predictions_query(input_table)
  _write_to_bigquery(
      hook_apply_formulas(_load_data_from_bq(query)), output_table)
  hook_on_completion()


def _test():
  message = {
      "bq_output_table":
          "ltv-framework",
      "bq_input_to_predict_table":
          "ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions",
      "date":
          "20210303",
      "operation": {
          "name":
              "projects/988912752389/locations/eu/operations/TBL8979557532418703360",
          "metadata": {
              "@type":
                  "type.googleapis.com/google.cloud.automl.v1beta1.OperationMetadata",
              "createTime":
                  "2021-03-05T17:57:54.251058Z",
              "updateTime":
                  "2021-03-05T18:02:43.797899Z",
              "batchPredictDetails": {
                  "inputConfig": {
                      "bigquerySource": {
                          "inputUri":
                              "bq://ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions_20210303"
                      }
                  },
                  "outputInfo": {
                      "bigqueryOutputDataset":
                          "bq://ltv-framework.prediction_training_data_20200605_0608_2021_03_05T09_57_54_169Z"
                  }
              }
          },
          "done":
              "true",
          "response": {
              "@type":
                  "type.googleapis.com/google.cloud.automl.v1beta1.BatchPredictResult"
          }
      }
  }
  msg_data = base64.b64encode(bytes(json.dumps(message).encode("utf-8")))

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
