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

"""Google Cloud function that sends batches of clients to predict."""

import base64
import datetime
import json
import logging
import os
import re
import sys
import uuid
# import pandas as pd
# import numpy as np

from typing import Any, Dict, Optional
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.api_core import exceptions as google_exception
from google.cloud.functions_v1.context import Context
from google.cloud import bigquery
import google.cloud.logging
from google.cloud import pubsub_v1
from google.cloud import firestore_v1 as firestore


import pytz
import datetime

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

DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME', '')
SOLUTION_PREFIX = os.getenv('SOLUTION_PREFIX', '')

MODEL_LOCATION = os.getenv('MODEL_LOCATION', '')
MODEL_AUTOML_API_ENDPOINT = os.getenv('MODEL_AUTOML_API_ENDPOINT', '')
MODEL_GCP_PROJECT = os.getenv('MODEL_GCP_PROJECT', '')

CLIENT_CLASS_MODULE = 'google.cloud.automl_v1beta1'
CLIENT_CLASS = 'AutoMlClient'

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

BQ_LTV_GCP_PROJECT = os.getenv('BQ_LTV_GCP_PROJECT', '')
BQ_LTV_DATASET = os.getenv('BQ_LTV_DATASET', '')

ENQUEUE_TASK_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                       os.getenv('ENQUEUE_TASK_TOPIC', ''))
PREDICT_TRANSACTIONS_BATCH_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                                     os.getenv('PREDICT_TRANSACTIONS_BATCH_TOPIC', ''))
PREDICTION_ERROR_HANDLER_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                                   os.getenv('PREDICTION_ERROR_HANDLER_TOPIC', ''))
POST_PROCESS_BATCH_PREDICTIONS_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                                 os.getenv('POST_PROCESS_BATCH_PREDICTIONS_TOPIC', ''))

DELAY_PREDICT_TRANSACTIONS_BATCH_IN_SECONDS = int(
    os.getenv('DELAY_PREDICT_TRANSACTIONS_BATCH_IN_SECONDS', '120'))
MAX_CONCURRENT_BATCH_PREDICT = int(
  os.getenv('MAX_CONCURRENT_BATCH_PREDICT', '5'))

BQ_LTV_TABLE_PREFIX = '{}.{}'.format(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_METADATA_TABLE = '{}.{}'.format(BQ_LTV_TABLE_PREFIX,
                                       os.getenv('BQ_LTV_METADATA_TABLE', ''))
DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME', '')
SOLUTION_PREFIX = os.getenv('SOLUTION_PREFIX', '')

FST_PREDICT_COLLECTION = '{}_{}_{}'.format(
    DEPLOYMENT_NAME,
    SOLUTION_PREFIX,
    os.getenv('FST_PREDICT_COLLECTION', ''))
COUNTER_DOCUMENT = 'concurrent_document'
COUNTER_FIELD = 'concurrent_count'
COUNTER_LAST_INSERT = 'last_insert'
BATCH_PREDICT_TIMEOUT = int(os.getenv('BATCH_PREDICT_TIMEOUT_SECONDS', ''))  # Counter timeout in seconds
GCP_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
API_VERSION = 'v1'


def _load_metadata(table):
  """Loads the metadata info from BQ.

  Args:
    table: A string representing the full address of the BQ table containing
      metadata

  Returns:
    A pandas dataframe containing the metadata info
  """

  query = f"""
      select a.model_date as model_date, b.model_id as model_id from (
        select format_date('%E4Y%m%d',min(date)) as model_date from (
          select max(PARSE_DATE('%E4Y%m%d', model_date)) as date  FROM {table}
          union all
          SELECT max(PARSE_DATE('%E4Y%m%d', model_date)) as date FROM {table}
            where
              DATE_DIFF(CURRENT_DATE(),
                PARSE_DATE('%E4Y%m%d', model_date), DAY) > 1
              and model_id is not null
        )
     ) as a left join {table} as b
     on a.model_date = b.model_date and b.model_date is not null
      """

  return bigquery.Client(project=BQ_LTV_GCP_PROJECT).query(query).to_dataframe().reset_index(drop=True)


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
  logger.debug(
      'Message sent to Pub/Sub. Project: %r, topic_path: %r, message: %r',
      project, topic_path, msg)


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


def _build_task_message(data, model_api_endpoint, operation_name, success_topic,
                        error_topic, source_topic, concurrent_slot_document):
  """Creates a JSON object to be sent to the long running operations system.

  The message contains all the details so the long running operation system
  is able to reconstruct the right API client and poll for the operation status.

  Args:
    data: A JSON object representing the payload of the original message
    model_api_endpoint: A string representing the API endpoint. Different for
      each region
    operation_name: A string representing the id of the operation to poll
    success_topic: A string representing the topic to where the initial message
      in case of success must be sent to
    error_topic: A string representing the topic to where the initial message in
      case of failure must be sent to
    source_topic: A string representing the topic from where the initial message
      was received
    concurrent_slot_document: Firestore document where concurrent slots are
      accounted for.

  Returns:
    The incoming msg JSON object containing all the input parameters together.
  """
  return {
      'status_check_url': f'https://{model_api_endpoint}/{API_VERSION}/{operation_name}',
      'status_field': 'state',
      'status_success_values': ['JOB_STATE_SUCCEEDED'],
      'status_error_values': ['JOB_STATE_FAILED', 'JOB_STATE_EXPIRED'],
      'payload': data,
      'error_topic': error_topic,
      'success_topic': success_topic,
      'source_topic': source_topic,
      'concurrent_slot_document': concurrent_slot_document,
  }


def _enqueue_operation_into_task_poller(gcp_project, data, model_api_endpoint,
                                        operation_name, success_topic,
                                        error_topic, source_topic,
                                        concurrent_slot_document):
  """It sends a message to the long running operations system.

  The long running operation system will forward the message back once it's done

  Args:
    gcp_project: A string representing the JSON project to use
    data: A JSON object representing the payload of the original message
    model_api_endpoint: A string representing the API endpoint. Different for
      each region
    operation_name: A string representing the id of the operation to poll
    success_topic: A string representing the topic to where the initial message
      in case of success must be sent to
    error_topic: A string representing the topic to where the initial message in
      case of failure must be sent to
    source_topic: A string representing the topic from where the initial message
      was received
    concurrent_slot_document: Firestore document where concurrent slots are
      accounted for.
  """
  msg = _build_task_message(data,
                            model_api_endpoint, operation_name, success_topic,
                            error_topic, source_topic, concurrent_slot_document)

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


def _start_processing(throttled, msg, model_gcp_project, model_location,
                      model_id, model_date, model_api_endpoint,
                      client_class_module, client_class, enqueue_topic,
                      success_topic, error_topic, source_topic, gcp_project,
                      delay_in_seconds):
  """Starts the message processing.

  There're 2 kind of messages:
    - thottled | non throttled

  Args:
    throttled: Boolean indicating if the message comes from the throttling
      system
    msg: JSON object representing the data to process
    model_gcp_project: String representing the name of the GCP project
    model_location: String representing the location of the prediction model
    model_id: String representing the ID of the prediction model to be
      used
    model_date: String representing the date of the model in YYYYMMDD format
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
    source_topic: String representing the topic where to the request was origina-
      lly receieved
      the case of success in the throttling operation
    gcp_project: String representing the GCP project to use for pub/sub and
      , firestore, etc...
    delay_in_seconds: Integer representing the delay time
  """

  logger.debug('Processing. Date: %s', msg['date'])
  db = firestore.Client(project=gcp_project)
  transaction = db.transaction()

  if _obtain_batch_predict_slot(transaction, db):

    try:
      job_name = _predict(model_id, model_gcp_project, model_location,
            model_api_endpoint, gcp_project,
            f"bq://{msg['bq_input_to_predict_table']}_{msg['date']}",
            f"bq://{msg['bq_output_table'].split('.')[0]}")

      
      _enqueue_operation_into_task_poller(gcp_project, msg, model_api_endpoint,
                                        job_name, success_topic,
                                        error_topic, source_topic,
                                        firestore.document.DocumentReference(
                                          FST_PREDICT_COLLECTION,
                                          COUNTER_DOCUMENT).path
                                        )
      
    except Exception:
      logger.exception('Error while processing the prediction for: %s_%s',
                       msg['bq_input_to_predict_table'], msg['date'])

  else:
    logger.debug('Throttling: %s', msg['date'])

    _throttle_message(gcp_project, msg, enqueue_topic, source_topic,
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


def _predict(model_id, model_gcp_project, model_location,
            model_api_endpoint, gcp_project, bq_input_uri, bq_output_uri):
  """It calls AutoML tables API to predict a batch os transactions

  Args:
    payload: A JSON object containing the data to be predicted

  Returns:
    Array containing the AutoML response with the predictions
  """
  credentials, project = google.auth.default(scopes=[GCP_SCOPE])

  authed_session = AuthorizedSession(credentials)

  request_url = f'https://{model_api_endpoint}/v1/projects/{model_gcp_project}/locations/{model_location}/batchPredictionJobs'
  request_data = json.dumps({
      'displayName': f'{DEPLOYMENT_NAME}_{SOLUTION_PREFIX}_batch_predict - {datetime.datetime.now()}',
      'inputConfig': {
          'instancesFormat': 'bigquery',
          'bigquerySource': {
              'inputUri': bq_input_uri
          }
      },
      'outputConfig': {
          'predictionsFormat': 'bigquery',
          'bigqueryDestination': {
              'outputUri': bq_output_uri
          }
      },
      'model': f'projects/{model_gcp_project}/locations/{model_location}/models/{model_id}'
  })
  logger.debug('Creating batch prediction. URL: %r, Data: %r',
               request_url, request_data)
  response = authed_session.post(request_url, data=request_data)
  response_dict = json.loads(response.text)
  logger.debug('Batch prediction created. Response: %r', response_dict)
  if 'error' in response_dict:
    raise Exception(f'Error while creating batch prediction job: {response_dict!r}')
  return response_dict['name']


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

  # counter = _get_distributed_counter()

  model_gcp_project = MODEL_GCP_PROJECT
  metadata_df = _load_metadata(BQ_LTV_METADATA_TABLE)
  model_date = str(metadata_df['model_date'][0])
  model_id = str(metadata_df['model_id'][0])
  model_location = MODEL_LOCATION
  model_api_endpoint = MODEL_AUTOML_API_ENDPOINT

  client_class_module = CLIENT_CLASS_MODULE
  client_class = CLIENT_CLASS
  enqueue_topic = ENQUEUE_TASK_TOPIC
  source_topic = PREDICT_TRANSACTIONS_BATCH_TOPIC
  success_topic = POST_PROCESS_BATCH_PREDICTIONS_TOPIC
  error_topic = PREDICTION_ERROR_HANDLER_TOPIC
  gcp_project = DEFAULT_GCP_PROJECT
  delay_in_seconds = DELAY_PREDICT_TRANSACTIONS_BATCH_IN_SECONDS
  data = base64.b64decode(event['data']).decode('utf-8')
  msg = json.loads(data)

  try:

    _start_processing(
        _is_throttled(event), msg, model_gcp_project, model_location, model_id,
        model_date, model_api_endpoint, client_class_module,
        client_class, enqueue_topic, success_topic, error_topic, source_topic,
        gcp_project, delay_in_seconds)

  # pylint: disable=bare-except
  except:
    logger.exception('Unexpected error.')
  # pylint: enable=bare-except

@firestore.transactional
def _obtain_batch_predict_slot(transaction, db):
  current_timestamp = datetime.datetime.now(pytz.utc)
  try:
    db.collection(FST_PREDICT_COLLECTION).document(COUNTER_DOCUMENT).create(
        {COUNTER_FIELD: 0,
         COUNTER_LAST_INSERT: current_timestamp,
         })
    logger.info('Counter document did not exist. Created: %s/%s',
                FST_PREDICT_COLLECTION,
                COUNTER_DOCUMENT)
  except google_exception.AlreadyExists:
    pass
  concurrent_ref = db.collection(FST_PREDICT_COLLECTION).document(COUNTER_DOCUMENT)
  snapshot = concurrent_ref.get(transaction=transaction)

  current_count = 0
  last_insert = datetime.datetime.min.replace(tzinfo=pytz.utc)
  try:
    current_count = snapshot.get(COUNTER_FIELD)
    last_insert = snapshot.get(COUNTER_LAST_INSERT)
  except KeyError:
    logger.info('Incomplete count information. Assuming no tasks executing.')

  logger.debug('Obtaining slot. Current count: %s. Last insert: %s',
               current_count, last_insert)
  if (current_count and current_timestamp - last_insert <
      datetime.timedelta(seconds = BATCH_PREDICT_TIMEOUT)):
    new_count = current_count + 1
  else:
    new_count = 1
  if new_count <= MAX_CONCURRENT_BATCH_PREDICT:
    transaction.update(concurrent_ref, {
        COUNTER_FIELD: new_count,
        COUNTER_LAST_INSERT: current_timestamp
    })
    return True
  else:
    logger.debug('Slot not obtained. Current count: %s. Last insert: %s',
                 current_count, last_insert)
    return False




def _first_call():
  """Test the processing of an initial message."""
  msg_data = {
      'bq_input_to_predict_table':
          'ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions',
      'bq_output_table':
          'ltv-framework.ltv_jaimemm.predictions',
      'date':
          '20210303'
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
          'decent-fulcrum-316414.test.filtered_periodic_transactions',
      'bq_output_table':
          'decent-fulcrum-316414.test.predictions',
      'date':
          '20210401'
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
