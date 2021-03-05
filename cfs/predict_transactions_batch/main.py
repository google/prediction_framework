"""Google Cloud function that sends batches of clients to predict."""

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
import re
import sys
import uuid

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import automl_v1beta1 as automl
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
import pytz

MODEL_REGION = os.getenv('MODEL_REGION', '')
MODEL_AUTOML_API_ENDPOINT = os.getenv('MODEL_AUTOML_API_ENDPOINT', '')
MODEL_GCP_PROJECT = os.getenv('MODEL_GCP_PROJECT', '')

CLIENT_CLASS_MODULE = 'google.cloud.automl_v1beta1'
CLIENT_CLASS = 'AutoMlClient'

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

BQ_LTV_GCP_PROJECT = os.getenv('BQ_LTV_GCP_PROJECT', '')
BQ_LTV_DATASET = os.getenv('BQ_LTV_DATASET', '')

ENQUEUE_TASK_TOPIC = os.getenv('ENQUEUE_TASK_TOPIC', '')
PREDICT_TRANSACTION_TOPIC = os.getenv('PREDICT_TRANSACTION_TOPIC', '')
PREDICT_TRANSACTIONS_BATCH_TOPIC = os.getenv('PREDICT_TRANSACTIONS_BATCH_TOPIC', '')
PREDICTION_ERROR_HANDLER_TOPIC = os.getenv('PREDICTION_ERROR_HANDLER_TOPIC', '')

DELAY_PREDICT_TRANSACTIONS_IN_SECONDS = int(
    os.getenv('DELAY_PREDICT_TRANSACTIONS_IN_SECONDS', '120'))

BQ_LTV_TABLE_PREFIX = '{}.{}'.format(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_METADATA_TABLE = '{}.{}'.format(BQ_LTV_TABLE_PREFIX,
                                       os.getenv('BQ_LTV_METADATA_TABLE', ''))

COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''), os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_PREDICT_TRANSACTIONS', ''))

def _insert_into_firestore(fs_project, fs_collection, processing_date):
  """Inserts a document into firestore.

  It uses current date time as key.

  Args:
    fs_project: A string representing the GCP project name
    fs_collection: A string representing the firestore collection name
    processing_date: A string representing the date in process. Format YYYYMMDD
  """
  now = datetime.datetime.now(pytz.utc)
  key = '{}_{}'.format(processing_date, now.strftime('%Y%m%d_%H%M%S'))
  print('generating key ', key)

  db = firestore.Client(project=fs_project)

  db.collection(fs_collection).document(key).set(
      {'inserted_timestamp': now})


def _load_metadata(table):
  """Loads the metadata info from BQ.

  Args:
    table: A string representing the full address of the BQ table containing
      metadata

  Returns:
    A pandas dataframe containing the metadata info
  """

  query = f"""
      select a.model_date as model_date, b.model_name as model_name from (
        select format_date('%E4Y%m%d',min(date)) as model_date from (
          select max(PARSE_DATE('%E4Y%m%d', model_date)) as date  FROM {table}
          union all
          SELECT max(PARSE_DATE('%E4Y%m%d', model_date)) as date FROM {table}
            where
              DATE_DIFF(CURRENT_DATE(),
                PARSE_DATE('%E4Y%m%d', model_date), DAY) > 1
              and model_name is not null
        )
     ) as a left join {table} as b
     on a.model_date = b.model_date and b.model_date is not null
      """

  return bigquery.Client().query(query).to_dataframe().reset_index(drop=True)


def _send_message(project, msg, topic_name):
  """Sends a message to the specified topic.

  Args:
    project: A string representing the GCP project to use
    msg: A JSON object to be sent to the topic
    topic_name: A string representing the name of the topic in the GCP project
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic_name)
  json_str = json.dumps(msg)

  _ = publisher.publish(topic_path, data=bytes(json_str, 'utf-8')).result()


def _send_message_to_self(project, msg):
  """Resends a message to this very cloud function.

  Args:
    project: A string representing the GCP project to use
    msg: A JSON object to be sent to the topic
  """
  _send_message(project, msg, PREDICT_TRANSACTIONS_BATCH_TOPIC)


def _send_message_task_enqueue(project, msg):
  """Sends a message to the throttling (enqueue) topic.

  Args:
    project: A string representing the GCP project to use
    msg: A JSON object to be sent to the topic
  """
  _send_message(project, msg, ENQUEUE_TASK_TOPIC)


def _build_task_message(data, client_class_module, client_class,
                        model_api_endpoint, operation_name, success_topic,
                        error_topic, source_topic):
  """Creates a JSON object to be sent to the long running operations system.

  The message contains all the details so the long running operation system
  is able to reconstruct the right API client and poll for the operation status.

  Args:
    data: A JSON object representing the payload of the original message
    client_class_module: A string representing the API module containing the
      client class to poll the GCP long running operation
    client_class: A string representing the API client class name to poll the
      GCP long running operation
    model_api_endpoint: A string representing the API endpoint. Different for
      each region
    operation_name: A string representing the id of the operation to poll
    success_topic: A string representing the topic to where the initial message
      in case of success must be sent to
    error_topic: A string representing the topic to where the initial message in
      case of failure must be sent to
    source_topic: A string representing the topic from where the initial message
      was received

  Returns:
    The incoming msg JSON object containing all the input parameters together.
  """
  client_class_module = 'google.cloud.automl_v1beta1'
  client_class = 'AutoMlClient'
  client_params = {'client_options': {'api_endpoint': model_api_endpoint}}
  return {
      'operation_name': operation_name,
      'client_class_module': client_class_module,
      'client_class': client_class,
      'client_params': client_params,
      'payload': data,
      'error_topic': error_topic,
      'success_topic': success_topic,
      'source_topic': source_topic
  }


def _enqueue_operation_into_task_poller(gcp_project, data, client_class_module,
                                        client_class, model_api_endpoint,
                                        operation_name, success_topic,
                                        error_topic, source_topic):
  """It sends a message to the long running operations system.

  The long running operation system will forward the message back once it's done

  Args:
    gcp_project: A string representing the JSON project to use
    data: A JSON object representing the payload of the original message
    client_class_module: A string representing the API module containing the
      client class to poll the GCP long running operation
    client_class: A string representing the API client class name to poll the
      GCP long running operation
    model_api_endpoint: A string representing the API endpoint. Different for
      each region
    operation_name: A string representing the id of the operation to poll
    success_topic: A string representing the topic to where the initial message
      in case of success must be sent to
    error_topic: A string representing the topic to where the initial message in
      case of failure must be sent to
    source_topic: A string representing the topic from where the initial message
      was received
  """
  msg = _build_task_message(data, client_class_module, client_class,
                            model_api_endpoint, operation_name, success_topic,
                            error_topic, source_topic)

  _send_message_task_enqueue(gcp_project, msg)

def _throttle_message(project, msg, enqueue_topic, success_topic, error_topic,
                      source_topic, delay):
  """Sends a message to the throttling system.

  Args:
    project: A string representing the JSON project to use
    msg: A JSON object representing the message to be throttled
    enqueue_topic: A string representing the topic to where the throttling
      system listens to
    success_topic: A string representing the topic to where the throttled
      message in case of success must be sent to
    error_topic: A string representing the topic to where the throttled message
      in case of failure must be sent to
    source_topic: A string representing the topic from where the throttled
      message was originally received
    delay: An integer representing the minimum amount of seconds the message
      will be held in the throttling system before being forwarded
  """

  new_msg = {
      'payload': msg,
      'operation_name': 'Delayed Forwarding',
      'delay_in_seconds': delay,
      'error_topic': error_topic,
      'success_topic': success_topic,
      'source_topic': source_topic
  }

  _send_message(project, new_msg, enqueue_topic)


def _start_processing(throttled, msg, model_gcp_project, model_name,
                      model_date, model_region, model_api_endpoint,
                      client_class_module, client_class, enqueue_topic,
                      success_topic, error_topic, gcp_project,
                      fs_collection, delay_in_seconds):
  """Starts the message processing.

  There're 2 kind of messages:
    - thottled | non throttled
    
  Args:
    throttled: Boolean indicating if the message comes from the throttling
      system
    msg: JSON object representing the data to process
    model_gcp_project: String representing the name of the GCP project
    model_name: String representing the name of the prediction model to be
      used
    model_date: String representing the date of the model in YYYYMMDD format
    model_region: String representing the region where the model resides
    model_api_endpoint: String representing the API endpoint of the model
      (different per region)
    client_class_module: String representing the API module containing the
      client class to poll the GCP long running operation
    client_class: String representing the API client class name to poll the
      GCP long running operation
    enqueue_topic: String representing the topic where the throttlign system
      listens to
    success_topic: String representing the topic where to forward the message in
      the case of success in the throttling operation  
    error_topic: String representing the topic where to forward the message in
      the case of failure in the throttling operation
    gcp_project: String representing the GCP project to use for pub/sub and
      firestore
    fs_collection: String representing the firestore collection to be used
    delay_in_seconds: Integer representing the minimum amount of time the
      message will be held in the throttling system
  """
  
  print('Inserting Firestore footprint for ', msg['date'])
  _insert_into_firestore(gcp_project, fs_collection, msg['date'])
  
  if throttled:
    try:
      operation = _predict(model_name, model_gcp_project, model_region, model_date,
            model_api_endpoint, gcp_project,
            f"{msg['bq_input_to_predict_table']}_{msg['date']}",
            msg['bq_output_table'])

      _enqueue_operation_into_task_poller(gcp_project, msg, client_class_module,
                                        client_class, model_api_endpoint,
                                        operation.name, chunk_topic,
                                        error_topic, chunk_topic)              
            
    except Exception as err:
      print(f"""Error while processing the prediction for 
      {msg['bq_input_to_predict_table']}_{msg['date']}
       {err}""")

  else:
    print(f"Throttling: {msg['date']}")
    
    _throttle_message(gcp_project, msg, enqueue_topic, success_topic,
                      error_topic, success_topic, delay_in_seconds)


def _is_throttled(event):
  """Checks if the message has been throttled already.

  Args:
    event: The pub/sub event object

  Returns:
    True if contains an attribute called "forwarded"
    False in any other case
  """
  return (event.get('attributes') is
          not None) and (event.get('attributes').get('forwarded') is not None)


def _predict(model_name, model_gcp_project, model_region,
            model_api_endpoint, gcp_project, bq_input_uri, bq_output_uri):
  """It calls AutoML tables API to predict a batch os transactions

  Args:
    payload: A JSON object containing the data to be predicted

  Returns:
    Array containing the AutoML response with the predictions
  """
  client_options = {"api_endpoint": model_api_endpoint}
  client = automl.TablesClient(
      project=model_gcp_project,
      region=model_region,
      client_options=client_options)

  execute = client.batch_predict(
      bigquery_input_uri=bq_input_uri,
      bigquery_output_uri=bq_output_uri,
      model_display_name=model_name)

  return execute.operation


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Triggers the message processing.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context

  model_gcp_project = MODEL_GCP_PROJECT
  metadata_df = _load_metadata(BQ_LTV_METADATA_TABLE)

  model_date = str(metadata_df['model_date'][0])
  model_name = str(metadata_df['model_name'][0])
  model_region = MODEL_REGION
  model_api_endpoint = MODEL_AUTOML_API_ENDPOINT

  client_class_module = CLIENT_CLASS_MODULE
  client_class = CLIENT_CLASS
  enqueue_topic = ENQUEUE_TASK_TOPIC
  success_topic = PREDICT_TRANSACTIONS_BATCH_TOPIC
  error_topic = PREDICTION_ERROR_HANDLER_TOPIC
  fs_collection = COLLECTION_NAME
  gcp_project = DEFAULT_GCP_PROJECT
  delay_in_seconds = DELAY_PREDICT_TRANSACTIONS_IN_SECONDS
  data = base64.b64decode(event['data']).decode('utf-8')
  msg = json.loads(data)

  try:
    _start_processing(
        _is_throttled(event), msg, model_gcp_project, model_name,
        model_region, model_api_endpoint, client_class_module,
        client_class, enqueue_topic, success_topic, error_topic, gcp_project,
        fs_collection, delay_in_seconds)
  # pylint: disable=bare-except
  except:
    print('Unexpected error:', sys.exc_info()[0])
  # pylint: enable=bare-except


def _first_call():
  """Test the processing of an initial message."""
  msg_data = {
      'bq_input_to_predict_table':
          'test.ltv_ml.prepared_new_customers_periodic_transactions',
      'bq_output_table':
          'test.ltv_ml.predictions',
      'date':
          '20200710'
  }

  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
      },
      context=None)


def _throttled_call():
  """Test a message which has been throttled."""

  msg_data = {
      'bq_input_to_predict_table':
          'test.ltv_ml.prepared_new_customers_periodic_transactions',
      'bq_output_table':
          'test.ltv_ml.predictions',
      'date':
          '20200824',
      'start_index':
          1,
      'end_index':
          50,
      'batch_size':
          1000,
      'total':
          21
  }

  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8'))),
          'attributes': {
              'forwarded': 'true'
          }
      },
      context=None)


if __name__ == '__main__':
  _throttled_call()