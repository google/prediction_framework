"""Google Cloud function to stop the AutoML model."""

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
from google.cloud.functions_v1.context import Context
from custom_functions import hook_post_stop_action
from google.cloud import automl_v1beta1
from google.cloud import bigquery
from google.cloud import firestore
import pytz

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')
COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''), os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_PREDICT_TRANSACTIONS', ''))

MODEL_REGION = os.getenv('MODEL_REGION', '')
MODEL_AUTOML_API_ENDPOINT = os.getenv('MODEL_AUTOML_API_ENDPOINT', '')
MODEL_GCP_PROJECT = os.getenv('MODEL_GCP_PROJECT', '')

BQ_LTV_DATASET = os.getenv('BQ_LTV_DATASET', '')

BQ_LTV_TABLE_PREFIX = '{}.{}'.format(DEFAULT_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_METADATA_TABLE = '{}.{}'.format(BQ_LTV_TABLE_PREFIX,
                                       os.getenv('BQ_LTV_METADATA_TABLE', ''))

STOP_MODEL_WAITING_MINUTES = int(os.getenv('STOP_MODEL_WAITING_MINUTES', '60'))

MODEL_POST_STOP_ACTION = os.getenv('MODEL_POST_STOP_ACTION', 'Y')


def _load_metadata(table):
  """Reads the mdoel metada from the BQ table.

  Args:
    table: A string representing the full path of the metadata BQ table.

  Returns:
    A pandas dataframe with the content of the BQ metadata table.
  """

  query = """SELECT
              a.model_date AS model_date,
              b.model_name AS model_name
            FROM (
              SELECT
                FORMAT_DATE('%E4Y%m%d',MIN(date)) AS model_date
              FROM (
                SELECT
                  MAX(PARSE_DATE('%E4Y%m%d',
                      model_date)) AS date
                FROM
                  {0}
                UNION ALL
                SELECT
                  MAX(PARSE_DATE('%E4Y%m%d',
                      model_date)) AS date
                FROM
                  {0}
                WHERE
                  DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%E4Y%m%d',
                      model_date), DAY) > 1
                  AND model_name IS NOT NULL ) ) AS a
            LEFT JOIN
              {0} AS b
            ON
              a.model_date = b.model_date
              AND b.model_date IS NOT NULL
        """

  query = query.format(table)

  return bigquery.Client().query(query).to_dataframe().reset_index(drop=True)


def _no_activity_in_minutes(last_activity, mins):
  """Checks no activity happened in the last mins minutes.

  Args:
    last_activity: A date time object representing the last time the activity
      was observed.
    mins: An integer representing the mins delta to add to last activity.

  Returns:
    True if current date time >= last_activity + mins.
    False otherwise.
  """
  current_date_time = datetime.datetime.now(pytz.utc)
  last_activity = last_activity + datetime.timedelta(minutes=mins)
  return last_activity <= current_date_time


def _no_firestore_activity(collection, waiting_time):
  """Checks if there's prediction activity by the age of firestore entries.

  Args:
    collection: A string representing the GCP collection to use.
    waiting_time: An integer representing the minutes to wait before considering
      no activity.

  Returns:
    True if no activity happened in the last waiting_time minutes.
    False otherwise.
  """
  docs = collection.order_by(
      u'inserted_timestamp',
      direction=firestore.Query.DESCENDING).limit(1).stream()
  doc_list = list(docs)
  if doc_list:
    doc = doc_list[0].to_dict()
    return _no_activity_in_minutes(doc['inserted_timestamp'], waiting_time)
  else:
    return True


def _delete_firestore_collection(collection, batch_size):
  """Deletes the firestore collection contents for housekeeping purposes.

  Args:
    collection: A string representing the GCP collection to use.
    batch_size: An integer representing the number of documents to delete on
      every run.

  Returns:
    A call to itself if still documents to delete.
    Nothing otherwise.
  """
  docs = collection.limit(batch_size).stream()
  deleted = 0

  for doc in docs:
    doc.reference.delete()
    deleted = deleted + 1

  if deleted >= batch_size:
    return _delete_firestore_collection(collection, batch_size)


def _get_model_status(model_gcp_project, model_name, model_region,
                      model_api_endpoint):
  """Queries the model status via de client API.

  Args:
    model_gcp_project: A string representing the GCP project to use.
    model_name: A string representing the name of the model.
    model_region: A string representing the API module containing the client
      class to poll the GCP long running operation.
    model_api_endpoint: A string representing the API endpoint. Different for
      each region.

  Returns:
    A string representing the status of the model.
  """
  client_options = {'api_endpoint': model_api_endpoint}

  client = automl_v1beta1.TablesClient(
      project=model_gcp_project,
      region=model_region,
      client_options=client_options)
  model = client.get_model(model_display_name=model_name)

  return model.deployment_state


def _is_deployed(model_gcp_project, model_name, model_region,
                 model_api_endpoint):
  """Returns True if the model is deployed, False otherwise.

  Args:
    model_gcp_project: A string representing the GCP project to use.
    model_name: A string  representing the name of the model.
    model_region: A string representing the API module containing the client
      class to poll the GCP long running operation.
    model_api_endpoint: A string representing the API endpoint. Different for
      each region.

  Returns:
    True if model is deployed, False otherwise.
  """
  state = _get_model_status(model_gcp_project, model_name, model_region,
                            model_api_endpoint)
  return state == automl_v1beta1.enums.Model.DeploymentState.DEPLOYED


def _stop_model(model_gcp_project, model_region, model_name,
                model_api_endpoint):
  """Undeploys the model.

  Args:
    model_gcp_project: A string representing the GCP project to use.
    model_region: A string representing the API module containing the client
      class to poll the GCP long running operation.
    model_name: A string  representing the name of the model.
    model_api_endpoint: A string representing the API endpoint. Different for
      each region.

  Returns:
    Long running operation object.
  """

  client_options = {'api_endpoint': model_api_endpoint}
  client = automl_v1beta1.TablesClient(
      project=model_gcp_project,
      region=model_region,
      client_options=client_options)

  execute = client.undeploy_model(model_display_name=model_name)

  return execute.operation


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Triggers the data processing corresponding to date in the eceived event.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  model_gcp_project = MODEL_GCP_PROJECT
  model_region = MODEL_REGION
  model_api_endpoint = MODEL_AUTOML_API_ENDPOINT
  metadata_df = _load_metadata(BQ_LTV_METADATA_TABLE)
  model_name = metadata_df['model_name'][0]
  waiting_time = STOP_MODEL_WAITING_MINUTES

  if _is_deployed(model_gcp_project, model_name, model_region,
                  model_api_endpoint):

    print('Model is deployed.')
    db = firestore.Client(DEFAULT_GCP_PROJECT)
    collection = db.collection(COLLECTION_NAME)
    print('Checking Firestore collection {} on project {}'.format(
        COLLECTION_NAME, DEFAULT_GCP_PROJECT))
    if _no_firestore_activity(collection, waiting_time):
      print('No prediction activity found. Stopping model.')
      _stop_model(model_gcp_project, model_region, model_name,
                  model_api_endpoint)
      if MODEL_POST_STOP_ACTION == 'Y':
        print('Executing post stop action')
        hook_post_stop_action(event, context)

      print('Deleting prediction activity log.')
      _delete_firestore_collection(collection, 40)
    else:
      print('Prediction activity in progress. Retry later.')
  else:
    print('Model is NOT deployed.')


def _normal_call():
  """Just triggers the main call."""
  msg_data = {}

  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
      },
      context=None)


if __name__ == '__main__':
  _normal_call()
