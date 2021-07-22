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

"""Google Cloud function code to extract only new customers transactions."""

import base64
import json
import os
import sys

from custom_functions import hook_get_bq_schema
from custom_functions import hook_get_load_data_query

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1

# pylint: disable=unused-import
import pandas as pd  # load_data_from_bq returns pandas dataframe
# pylint: enable=unused-import

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME', '')
SOLUTION_PREFIX = os.getenv('SOLUTION_PREFIX', '')
PREPARE_COLLECTION_NAME = '{}_{}_{}'.format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
    os.getenv('FST_PREPARE_COLLECTION', ''))

BQ_LTV_GCP_PROJECT = os.getenv('BQ_LTV_GCP_PROJECT', '')
BQ_LTV_DATASET = os.getenv('BQ_LTV_DATASET', '')
BQ_LTV_TABLE_PREFIX = '{}.{}'.format(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_TABLES_METADATA = '{}.__TABLES__'.format(BQ_LTV_TABLE_PREFIX)

BQ_LTV_PREPARED_PERIODIC_TX_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX,
    os.getenv('BQ_LTV_PREPARED_PERIODIC_TX_TABLE', 'prepared_periodic_transactions'))

BQ_LTV_FILTERED_TX_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX, os.getenv('BQ_LTV_FILTERED_TX_TABLE',
                                   ''))

BQ_LTV_PREDICTIONS_TABLE_ONLY = os.getenv('BQ_LTV_PREDICTIONS_TABLE', '')

OUTPUT_BQ_PREPARED_TX_DATA_TABLE_PREFIX = BQ_LTV_FILTERED_TX_TABLE

OUTPUT_BQ_PREDICTIONS_DATA_TABLE_PREFIX = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX, BQ_LTV_PREDICTIONS_TABLE_ONLY)

INBOUND_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                  os.getenv('DATA_PREPARED_TOPIC', ''))

ENQUEUE_TASK_TOPIC = "{}.{}.{}".format(DEPLOYMENT_NAME, SOLUTION_PREFIX,
                                       os.getenv('ENQUEUE_TASK_TOPIC', ''))

DELAY_IN_SECONDS = int(
    os.getenv('DELAY_FILTER_TRANSACTIONS_PERIODIC_IN_SECONDS', '-1'))


def _load_data_from_bq(table, current_date):
  """Retrieves data from BQ table correspoinding to new customers.

  Args:
    table: the dataframe containing the data to be written.
    current_date: the BQ table where the data must be written.

  Returns:
    A dataframe containing the data
  """
  query = hook_get_load_data_query(table, current_date)

  job_config = bigquery.job.QueryJobConfig()

  df = bigquery.Client().query(query, job_config=job_config).to_dataframe()
  return df


def _write_to_bigquery(df, table_id):
  """Writes the contents of the dataframe into the specified BQ table.

  Args:
    df: The dataframe containing the data to be written.
    table_id: The BQ table where the data must be written.
  """

  client = bigquery.Client()

  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = 'WRITE_TRUNCATE'
  job_config.schema = hook_get_bq_schema()

  job = client.load_table_from_dataframe(
      df, table_id, job_config=job_config)  # Make an API request.
  job.result()  # Wait for the job to complete.

  table = client.get_table(table_id)  # Make an API request.
  print('Loaded {} rows and {} columns to {}'.format(table.num_rows,
                                                     len(table.schema),
                                                     table_id))


def _get_date(msg):
  """Returns the date included into the message.

  Args:
    msg: A json message.

  Returns:
    The date string
  """

  return msg['date']


def _build_message(input_table, output_table, current_date):
  """Returns a JSON object containing the values of the input parameters.

  Args:
    input_table: A string containing the BQ table to read from
    output_table: A string containing the BQ table to write to
    current_date: The processing date to suffix the table names

  Returns:
    JSON oject containing the input data
  """
  msg = {
      'bq_input_to_predict_table': input_table,
      'bq_output_table': output_table,
      'date': current_date
  }

  return msg


def _send_message(project, msg, topic):
  """Sends a pub/sub message to the specified topic.

  Args:
    project: String representing the GCP project to use
    msg: JSON object to publish
    topic: The topic to publish the message to
  """

  publisher = pubsub_v1.PublisherClient()

  topic_path = publisher.topic_path(project, topic)

  msg_json = json.dumps(msg)

  _ = publisher.publish(
      topic_path,
      data=bytes(msg_json, 'utf-8'),
  ).result()


def _throttle_message(project, msg, enqueue_topic, success_topic, error_topic,
                      source_topic, delay):
  """Sends a message to the throttling system.

  Args:
    project: String representing the GCP project to use
    msg: JSON object to publish
    enqueue_topic: The topic to publish the messate where the throttling system
      listens to
    success_topic: The topic to forward the message after throttling
    error_topic: The topoc to forward the message in case of poblems during
      throttling
    source_topic: The original topic from where the message was received
    delay: The minimum delay in seconds the message needs
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
  print('Message published for {} to {}'.format(msg['date'], enqueue_topic))


def _is_prepare_running(project, collection):
  """Checks if the prepare periodic transactions phase is still running.

  New customers calculation cannot start without getting all the history
  first.

  Args:
    project: The GCP project where the firestore collection is located
    collection: The firestore collection where the prepare periodic transactions
      phase logs activity

  Returns:
    True if activity is logged, False in any other case
  """

  db = firestore.Client(project)
  task = db.collection(collection).limit(1).get()

  if not task:
    return False
  else:
    return True


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Triggers the extraction of new customer data.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context
  try:
    data = base64.b64decode(event['data']).decode('utf-8')
    msg = json.loads(data)
    current_date = _get_date(msg)
    print('Received: {}'.format(current_date))

    project = DEFAULT_GCP_PROJECT
    fst_collection = PREPARE_COLLECTION_NAME
    inbound_topic = INBOUND_TOPIC
    error_topic = ''
    source_topic = inbound_topic
    outbound_topic = "{}.{}.{}".format(
        DEPLOYMENT_NAME, SOLUTION_PREFIX,
        os.getenv('PREDICT_TRANSACTIONS_BATCH_TOPIC', ''))
    enqueue_topic = ENQUEUE_TASK_TOPIC
    delay = DELAY_IN_SECONDS

    if (event.get('attributes') is
        not None) and (event.get('attributes').get('forwarded') is not None):

      print('Processing: {}'.format(current_date))
      if not _is_prepare_running(project, fst_collection):

        input_bq_transactions_table = '{}_*'.format(
            BQ_LTV_PREPARED_PERIODIC_TX_TABLE)

        output_bq_prepared_tx_data_table = '{}_{}'.format(
            OUTPUT_BQ_PREPARED_TX_DATA_TABLE_PREFIX, current_date)

        df = _load_data_from_bq(input_bq_transactions_table, current_date)
        if len(df) > 0:
          _write_to_bigquery(df, output_bq_prepared_tx_data_table)
          msg = _build_message(OUTPUT_BQ_PREPARED_TX_DATA_TABLE_PREFIX,
                               OUTPUT_BQ_PREDICTIONS_DATA_TABLE_PREFIX,
                               current_date)
          _send_message(project, msg, outbound_topic)
          print('Message published for {} to {}'.format(current_date,
                                                        outbound_topic))
        else:
          print('No new customers found. Skipping table creation')
      else:
        print('Throttling, wait for previous task: {}'.format(current_date))
        _throttle_message(project, msg, enqueue_topic, inbound_topic,
                          error_topic, source_topic, delay)
    else:
      print('First Throttling: {}'.format(current_date))
      _throttle_message(project, msg, enqueue_topic, inbound_topic, error_topic,
                        source_topic, delay)
  except:
    print('Unexpected error: {}'.format(sys.exc_info()[0]))
    raise


if __name__ == '__main__':

  msg_data = {'date': '20200710'}
  msg_data = base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
  main(event={'data': msg_data, 'attributes': {'forwarded': 'true'}})
