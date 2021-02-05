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

from custom_functions import hook_get_load_batch_query

from google.api_core.exceptions import FailedPrecondition
from google.cloud import automl_v1beta1
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
import google.cloud.logging
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
PREDICT_TRANSACTIONS_TOPIC = os.getenv('PREDICT_TRANSACTIONS_TOPIC', '')
PREDICTION_ERROR_HANDLER_TOPIC = os.getenv('PREDICTION_ERROR_HANDLER_TOPIC', '')

DELAY_PREDICT_TRANSACTIONS_IN_SECONDS = int(
    os.getenv('DELAY_PREDICT_TRANSACTIONS_IN_SECONDS', '120'))

BQ_LTV_TABLE_PREFIX = '{}.{}'.format(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_METADATA_TABLE = '{}.{}'.format(BQ_LTV_TABLE_PREFIX,
                                       os.getenv('BQ_LTV_METADATA_TABLE', ''))

MAX_PREDICTION_BATCH_SIZE = int(os.getenv('MAX_PREDICTION_BATCH_SIZE', '10'))

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

  _ = db.collection(fs_collection).document(key).set(
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
  _send_message(project, msg, PREDICT_TRANSACTIONS_TOPIC)


def _send_message_to_predict_tx(project, msg):
  """Sends a message to the predict transaction topic.

  Args:
    project: A string representing the GCP project to use
    msg: A JSON object to be sent to the topic
  """
  _send_message(project, msg, PREDICT_TRANSACTION_TOPIC)


def _send_message_task_enqueue(project, msg):
  """Sends a message to the throttling (enqueue) topic.

  Args:
    project: A string representing the GCP project to use
    msg: A JSON object to be sent to the topic
  """
  _send_message(project, msg, ENQUEUE_TASK_TOPIC)


def _build_predict_tx_message(msg, msg_uuid, row, model_name, model_gcp_project,
                              model_region, model_date, model_api_endpoint):
  """Creates a JSON object for predict transaction cloud function.

  Args:
    msg: A JSON object representing the original message received by this cloud
      function
    msg_uuid: A string representing the unique identifier of the processing
      batch
    row: An array representing the data to be predicted
    model_name: A string representing the name of the prediction model to be
      used
    model_gcp_project: A string representing the name of the GCP project
    model_region: A string representing the region where the model resides
    model_date: A string representing the date of the model
    model_api_endpoint: A string representing the API endpoint of the model
      (different per region)

  Returns:
    A JSON object containing all the input parameters together.
  """
  return {
      'uuid': msg_uuid,
      'row': row,
      'model_project': model_gcp_project,
      'model_region': model_region,
      'model_api_endpoint': model_api_endpoint,
      'model_name': model_name,
      'model_date': model_date,
      'bq_output_table': msg['bq_output_table'] + '_' + msg['date'],
  }


def _build_self_message(msg, start, end, batch_size, total):
  """Creates a JSON object for batch processing.

  The function builds the message with all the details to be able to process the
  batch data when received. The aim of this message is this very same cloud
  function.

  Args:
    msg: A JSON object representing the message to modify function
    start: An integer representing the start index from the total ocurrences
    end: An integer representing the end index from the total ocurrences
    batch_size: An integer representing the amount of ocurrences in the batch
    total: An integer representing the total amount of ocurrences

  Returns:
    The incoming msg JSON object containing all the input parameters together.
  """
  msg['start_index'] = start
  msg['end_index'] = end
  msg['batch_size'] = batch_size
  msg['total'] = total

  return msg


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


def _handle_precondition(text):
  """Checks if the model status text indicates it is running.

  Args:
    text: A string representing the text containign the model status

  Returns:
    A tuple
      done = True if the model is already deployed
      done = False any other case
      operation_name = <id> if the model is not deployed
      operation_name = None if the model is deployed

  Raises:
    Exception: generic error just to stop the process.
  """

  operation_name = ''
  done = False
  match = re.search(':(.*).', text, re.IGNORECASE)
  if match:
    operation_name = match.group(1)
  else:
    match = re.search('(.*)400 The model is already deployed(.*)', text,
                      re.IGNORECASE)
    if match:
      operation_name = None
      done = True
    else:
      raise Exception('Unknown model status. Check API did not change. ', text)
  return done, operation_name.strip()


def _get_model_status(model_gcp_project, model_name, model_region,
                      model_api_endpoint):
  """Queries the model status via de client API.

  Args:
    model_gcp_project: A string representing the GCP project to use
    model_name: A string  representing the name of the model.
    model_region: A string representing the API module containing the client
      class to poll the GCP long running operation
    model_api_endpoint: A string representing the API endpoint. Different for
      each region

  Returns:
    A string representing the status of the model
  """

  client_options = {'api_endpoint': model_api_endpoint}
  client = automl_v1beta1.TablesClient(
      project=model_gcp_project,
      region=model_region,
      client_options=client_options)

  model = client.get_model(model_display_name=model_name)

  return model.deployment_state


def _check_model(model_gcp_project, model_name, model_region,
                 model_api_endpoint):
  """Checks the status of the model.

  If the model it is not running and not deploying, it will trigger the model
  deployment.

  Args:
    model_gcp_project: A string representing the GCP project to use
    model_name: A string  representing the name of the model.
    model_region: A string representing the API module containing the client
      class to poll the GCP long running operation
    model_api_endpoint: A string representing the API endpoint. Different for
      each region

  Returns:
    A tuple containing if the model is deployed or not and long running
    operation
    id if not
  """
  state = _get_model_status(model_gcp_project, model_name, model_region,
                            model_api_endpoint)
  if state == automl_v1beta1.enums.Model.DeploymentState.DEPLOYED:
    done = True
    name = ''
    return done, name
  else:
    try:
      operation = _start_model(model_gcp_project, model_name, model_region,
                               model_api_endpoint)
      name = operation.name
      done = False
      return done, name
    except FailedPrecondition as err:
      done, name = _handle_precondition(str(err))
      return done, name


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


def _start_model(model_gcp_project, model_name, model_region,
                 model_api_endpoint):
  """Starts the model.

  Args:
    model_gcp_project: A string representing the name of the GCP project
    model_name: A string representing the name of the prediction model to be
      used
    model_region: A string representing the region where the model resides
    model_api_endpoint: A string representing the API endpoint of the model
      (different per region)

  Returns:
    A long running operations object
  """

  client_options = {'api_endpoint': model_api_endpoint}
  client = automl_v1beta1.TablesClient(
      project=model_gcp_project,
      region=model_region,
      client_options=client_options)

  execute = client.deploy_model(model_display_name=model_name)

  return execute.operation


def _start_processing(throttled, msg, chunk_size, model_gcp_project, model_name,
                      model_date, model_region, model_api_endpoint,
                      client_class_module, client_class, chunk_topic,
                      enqueue_topic, error_topic, gcp_project, fs_collection,
                      delay_in_seconds):
  """Starts the message processing.

  There're 2 kind of messages:
    - initial throttled | non throttled
    - chunk throttled | non throttled

  Args:
    throttled: A boolean indicating if the message comes from the throttling
      system
    msg: A JSON object representing the data to process
    chunk_size: the number of occurrencies in the chunk
    model_gcp_project: A string representing the name of the GCP project
    model_name: A string representing the name of the prediction model to be
      used
    model_date: A string representing the date of the model in YYYYMMDD format
    model_region: A string representing the region where the model resides
    model_api_endpoint: A string representing the API endpoint of the model
      (different per region)
    client_class_module: A string representing the API module containing the
      client class to poll the GCP long running operation
    client_class: A string representing the API client class name to poll the
      GCP long running operation
    chunk_topic: A string representing the topic where the chunk processor
      listens to (this very cloud function, in fact)
    enqueue_topic: A string representing the topic where the throttlign system
      listens to
    error_topic: A string representing the topic where to forward the message in
      the case of failure
    gcp_project: A string representing the GCP project to use for pub/sub and
      firestore
    fs_collection: A string representing the firestore collection to be used
    delay_in_seconds: An integer representing the minimum amount of time the
      message will be held in the throttling system
  """
  done, operation_name = _check_model(model_gcp_project, model_name,
                                      model_region, model_api_endpoint)

  print('Inserting Firestore footprint for ', msg['date'])
  _insert_into_firestore(gcp_project, fs_collection, msg['date'])

  if done:
    if 'start_index' in msg:
      print('Processing chunk: {} between {} and {}'.format(
          msg['date'], msg['start_index'], msg['end_index']))

      if throttled:
        _process_chunk(msg, msg['start_index'], msg['end_index'], model_name,
                       model_gcp_project, model_region, model_date,
                       model_api_endpoint, gcp_project)

      else:
        print('Throttling: {} between {} and {}'.format(msg['date'],
                                                        str(msg['start_index']),
                                                        str(msg['end_index'])))
        _throttle_message(gcp_project, msg, enqueue_topic, chunk_topic,
                          error_topic, chunk_topic, delay_in_seconds)
    else:
      print('Splitting into chunks: {}'.format(msg['date']))
      _delete_table('{}_{}'.format(msg['bq_output_table'], msg['date']))
      _split_into_chunks(gcp_project, msg, chunk_size)
  else:
    print('Enqueuing: {}'.format(msg['date']))
    _enqueue_operation_into_task_poller(gcp_project, msg, client_class_module,
                                        client_class, model_api_endpoint,
                                        operation_name, chunk_topic,
                                        error_topic, chunk_topic)


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

    _ = client.query(query).to_dataframe()  # Make an API request.
  # pylint: disable=bare-except
  except:
    print('Error happened while deleting the table {}'.format(table_path))
  # pylint: enable=bare-except


def _split_into_chunks(default_project, msg, chunk_size):
  """It divides the total occurrences of the message into chunks of chunk_size.

  Args:
    default_project: A string representing the GCP project to use
    msg: A JSON object representing the input message
    chunk_size: An integer representing the size of the chunk
  """

  table_name = '{}_{}'.format(msg['bq_input_to_predict_table'], msg['date'])
  client = bigquery.Client()
  query = f"""
    SELECT
    count(1) as c
    FROM {table_name}"""

  df = client.query(query).to_dataframe()  # Make an API request.
  json_obj = json.loads(df.to_json(orient='records'))

  limit = int(json_obj[0]['c'])
  accrued = int(1)

  while accrued <= limit:
    start = accrued
    end = min(limit, accrued + chunk_size)
    print('{} start_index={} end_index={}'.format(msg['date'], str(start),
                                                  str(end)))

    _send_message_to_self(
        default_project, _build_self_message(msg, start, end, chunk_size,
                                             limit))
    accrued = end + 1


def _process_chunk(msg, start, end, model_name, model_gcp_project, model_region,
                   model_date, model_api_endpoint, default_project):
  """Process a chunk of clients.

  It reads the BQ table from start to end, and sends each line to the single row
  prediction cloud function

  Args:
    msg: A JSON object representing the input data
    start: An integer representing the start index to start reading from the
      table
    end: An integer representing the end index to stop reading from the table
    model_name: A string representing the name of the prediction model to be
      used
    model_gcp_project: A string representing the GCP project where the model is
    model_region: A string representing the region where the model resides
    model_date: A string representing the date of the model in YYYYMMDD format
    model_api_endpoint: A string representing the API endpoint of the model
      (different per region)
    default_project: A string representing the GCP project to use for pub/sub &
      bigquery
  """
  client = bigquery.Client()
  table = '{}_{}'.format(msg['bq_input_to_predict_table'], msg['date'])
  query = hook_get_load_batch_query(table, start, end)

  df = client.query(query).to_dataframe()  # Make an API request.
  json_obj = json.loads(df.to_json(orient='records'))

  tx_set_uuid = str(uuid.uuid4())

  for row in json_obj:
    new_msg = _build_predict_tx_message(msg, tx_set_uuid, row, model_name,
                                        model_gcp_project, model_region,
                                        model_date, model_api_endpoint)

    _send_message_to_predict_tx(default_project, new_msg)


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


def main(event, context=None):
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

  # Instantiates a client
  client = google.cloud.logging.Client()

  # Retrieves a Cloud Logging handler based on the environment
  # you're running in and integrates the handler with the
  # Python logging module. By default this captures all logs
  # at INFO level and higher
  client.get_default_handler()
  client.setup_logging()

  model_gcp_project = MODEL_GCP_PROJECT
  metadata_df = _load_metadata(BQ_LTV_METADATA_TABLE)

  model_date = str(metadata_df['model_date'][0])
  model_name = str(metadata_df['model_name'][0])
  model_region = MODEL_REGION
  model_api_endpoint = MODEL_AUTOML_API_ENDPOINT

  client_class_module = CLIENT_CLASS_MODULE
  client_class = CLIENT_CLASS
  chunk_topic = PREDICT_TRANSACTIONS_TOPIC
  enqueue_topic = ENQUEUE_TASK_TOPIC
  error_topic = PREDICTION_ERROR_HANDLER_TOPIC
  fs_collection = COLLECTION_NAME
  batch_size = MAX_PREDICTION_BATCH_SIZE
  gcp_project = DEFAULT_GCP_PROJECT
  delay_in_seconds = DELAY_PREDICT_TRANSACTIONS_IN_SECONDS
  data = base64.b64decode(event['data']).decode('utf-8')
  msg = json.loads(data)

  try:
    _start_processing(
        _is_throttled(event), msg, batch_size, model_gcp_project, model_name,
        model_date, model_region, model_api_endpoint, client_class_module,
        client_class, chunk_topic, enqueue_topic, error_topic, gcp_project,
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


def _chunk_call():
  """Test the processing of chunk message."""

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
