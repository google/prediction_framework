"""Google Cloud function code to predicts single customer LTV."""

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
import datetime
import json
import os
from typing import Any, Dict, Optional

from absl import logging
from custom_functions import hook_apply_formulas
from custom_functions import hook_create_row_to_insert
from custom_functions import hook_get_bq_schema
from google.cloud import automl_v1beta1 as automl
from google.cloud import bigquery
from google.cloud import firestore
import google.cloud.functions.Context
import pytz

_DEFAULT_GCP_PROJECT = os.getenv("DEFAULT_GCP_PROJECT", "")
_COLLECTION_NAME = "{}_{}_{}".format(
    os.getenv("DEPLOYMENT_NAME", ""), os.getenv("SOLUTION_PREFIX", ""),
    os.getenv("FST_PREDICT_UNIT_COLLECTION", ""))


def _stream_write_bq(row, bq_table, schema):
  """Writes to BQ in stream mode.

  Args:
    row: An array containing the information to be written according to the
      table schema
    bq_table: Full address of the BQ table
    schema: The schema of the BQ table
  """

  client = bigquery.Client()
  table = bigquery.Table(bq_table, schema=schema)
  table = client.create_table(table, True)
  rows_to_insert = [row]
  errors = client.insert_rows(table, rows_to_insert, selected_fields=schema)

  if errors:
    logging.warning("Error happened: %s", str(errors))


def _create_key(payload: Dict[str, Any]) -> str:
  """Returns a string to be used as key for storing into Firestore.

  Args:
    payload: A JSON object with the information to use to generate the key.

  Returns:
    A string representing the key.
  Raises:
    KeyError: error indicating the necessary fields are not included into the
    message payload.
  """
  try:
    return "{}_{}_{}".format(payload["bq_output_table"], payload["uuid"],
                             str(payload["row"]["rowId"]))
  except KeyError:
    raise KeyError(
        "At least one of the following fields is missing in the message: uuid, bq_output_table, row"
    )


def _insert_into_firestore(project: str, collection_name: str,
                           payload: Dict[str, Any]):
  """Inserts a document into a Firestore collection.

  Args:
    project: A String represeting the GCP project to use
    collection_name: A String representing the Firestore collection to use
    payload: A JSON object representing the document payload to be store in
      Firestore
  """
  firestore.Client(project).collection(collection_name).document(
      _create_key(payload)).set({
          "inserted_timestap": datetime.datetime.now(pytz.utc),
          "payload": payload
      })


def _remove_from_firestore(project: str, collection_name: str,
                           payload: Dict[str, Any]):
  """Removes a document from Firestore collection.

  Args:
    project: A String represeting the GCP project to use
    collection_name: A String representing the Firestore collection to use
    payload: A JSON object representing the document payload stored in Firestore
  """
  firestore.Client(project).collection(collection_name).document(
      _create_key(payload)).delete()


def _predict(payload: Dict[str, Any]):
  """It calls AutoML tables API to predict a customer transaction.

  Args:
    payload: A JSON object containing the data to be predicted

  Returns:
    Array containing the AutoML response with the predictions
  """
  client_options = {"api_endpoint": payload["model_api_endpoint"]}
  client = automl.TablesClient(
      project=payload["model_project"],
      region=payload["model_region"],
      client_options=client_options)

  return client.predict(
      model_display_name=payload["model_name"], inputs=payload["row"])


def main(event: Dict[str, Any],
         context=Optional[google.cloud.functions.Context]):
  """Triggers the prediction of a customer transaction.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context
  data = base64.b64decode(event["data"])
  payload = json.loads(data)
  collection_name = "{}_{}".format(_COLLECTION_NAME, payload["bq_output_table"])

  print("Processing chunk {} with target {}".format(payload["uuid"],
                                                    payload["bq_output_table"]))

  fst_project = _DEFAULT_GCP_PROJECT
  _insert_into_firestore(fst_project, collection_name, payload)

  prediction_result = _predict(payload)
  predicted_values = hook_apply_formulas(payload["row"], prediction_result)
  new_row = hook_create_row_to_insert(payload["row"], predicted_values)

  _stream_write_bq(new_row, payload["bq_output_table"], hook_get_bq_schema())
  _remove_from_firestore(fst_project, collection_name, payload)


# In case you'd want to test it using your python virtual environment.
if __name__ == "__main__":

  message_payload = {
      "uuid": "343af019-1615-47dc-b4c2-6ee17b5c9dde",
      "row": {},
      "model_project": "test",
      "model_region": "eu",
      "model_api_endpoint": "eu-automl.googleapis.com:443",
      "model_name": "training_data_20200605_0608",
      "model_date": "20200605",
      "bq_output_table": "test.ltv_ml.predictions_20200917"
  }

  main(
      event={
          "data":
              base64.b64encode(
                  bytes(json.dumps(message_payload).encode("utf-8")))
      },
      context=None)
