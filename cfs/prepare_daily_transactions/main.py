"""Google Cloud function that aggregates client transactions into single line."""

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
import sys
from typing import Any, Dict, Optional

from custom_functions import hook_get_bq_schema
from custom_functions import hook_get_load_tx_query
from custom_functions import hook_prepare
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
import google.cloud.functions.Context
import pytz

COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''), os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_PREPARE_COLLECTION', ''))

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')
BQ_LTV_GCP_PROJECT = os.getenv('BQ_LTV_GCP_PROJECT', '')
BQ_LTV_DATASET = os.getenv('BQ_LTV_DATASET', '')
BQ_LTV_TABLE_PREFIX = '{}.{}'.format(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET)
BQ_LTV_TABLES_METADATA = '{}.__TABLES__'.format(BQ_LTV_TABLE_PREFIX)

BQ_LTV_ALL_DAILY_TX_TABLE_ONLY = os.getenv('BQ_LTV_ALL_DAILY_TX_TABLE', '')
BQ_LTV_ALL_DAILY_TX_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX, os.getenv('BQ_LTV_ALL_DAILY_TX_TABLE', ''))
BQ_LTV_PREPARED_DAILY_TX_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX, os.getenv('BQ_LTV_PREPARED_DAILY_TX_TABLE', ''))
BQ_LTV_PREPARED_NEW_CUSTOMERS_TX_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX, os.getenv('BQ_LTV_PREPARED_NEW_CUSTOMERS_TX_TABLE',
                                   ''))

BQ_LTV_METADATA_TABLE = '{}.{}'.format(BQ_LTV_TABLE_PREFIX,
                                       os.getenv('BQ_LTV_METADATA_TABLE', ''))

MODEL_REGION = os.getenv('MODEL_REGION', '')

INBOUND_TOPIC = os.getenv('DATA_EXTRACTED_TOPIC', '')
OUTBOUND_TOPIC = os.getenv('DATA_PREPARED_TOPIC', '')
ENQUEUE_TASK_TOPIC = os.getenv('ENQUEUE_TASK_TOPIC', '')

DELAY_IN_SECONDS = int(os.getenv('DELAY_PREPARE_DAILY_IN_SECONDS', '120'))


def _load_metadata(table):
  """Reads the mdoel metada from the BQ table.

  Args:
    table: A string representing the full path of the metadata BQ table

  Returns:
    A pandas dataframe with the content of the BQ metadata table
  """

  query = f"""SELECT
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
                  {table}
                UNION ALL
                SELECT
                  MAX(PARSE_DATE('%E4Y%m%d',
                      model_date)) AS date
                FROM
                  {table}
                WHERE
                  DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%E4Y%m%d',
                      model_date), DAY) > 1
                  AND model_name IS NOT NULL ) ) AS a
            LEFT JOIN
              {table} AS b
            ON
              a.model_date = b.model_date
              AND b.model_date IS NOT NULL
        """

  return bigquery.Client().query(query).to_dataframe().reset_index(drop=True)


def _write_to_bigquery(df, table_name):
  """Writes the given dataframe into the BQ table.

  Args:
    df: A pandas dataframe representing the data to be written
    table_name: A string representing the full path of the metadata BQ table
  """

  dataframe = df

  col_list = list(dataframe)

  dataframe = df[col_list]
  dataframe = dataframe.drop(columns={'mobileDeviceBranding'})

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


def _check_table(meta_data_table, table):
  """Checks if the table exists and the content.

  Args:
    meta_data_table: A string representing the full path of the metadata BQ
      table
    table: A string representing the table to check

  Returns:
    An integer representing the status of the table:
    0 => table exists and not empty
    1 => table exists but empty
    2 => table does not exist
  """
  query = f"""
      SELECT size_bytes FROM `{meta_data_table}` where  table_id = "{table}"; """

  job_config = bigquery.job.QueryJobConfig()

  df = bigquery.Client().query(query, job_config=job_config).to_dataframe()

  if not df['size_bytes']:
    return 2  # table does not exist
  else:
    if int(df['size_bytes'][0]) <= 0:
      return 1  # table exists but is empty
    else:
      return 0  # table exists and is not empty


def _load_tx_data_from_bq(table):
  """Loads all the transactions from the table.

  Args:
    table: A string representing the full table path

  Returns:
    A dataframe with all the table data
  """
  query = hook_get_load_tx_query(table)

  job_config = bigquery.job.QueryJobConfig()

  return bigquery.Client().query(query, job_config=job_config).to_dataframe()


def _get_date(msg):
  """Extracts the date from the message.

  Args:
    msg: A JSON object representing the message

  Returns:
    A string representing the date of the data to be processed in YYYYMMDD
    format
  """

  date = datetime.datetime.strptime(msg['runTime'], '%Y-%m-%dT%H:%M:%SZ')

  return date.strftime('%Y%m%d')


def _send_message(project, msg, topic):
  """Sends the message to the topic in project.

  Args:
    project: A string representing the GCP project to use for pub/sub
    msg: A JSON object representing the message to be sent
    topic: A string representing the topic name
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic)

  msg_json = json.dumps(msg)

  unused_msg_id = publisher.publish(
      topic_path,
      data=bytes(msg_json, 'utf-8'),
  ).result()


def _send_message_to_next_stage(project, topic, processing_date):
  """Sends the message to the next stage in the pipeline, indicated by topic.

  Args:
    project: A string representing the GCP project to use for pub/sub
    topic: A string representing the topic name
    processing_date: A string representing the date of the date to be processed
      in YYYYMMDD format
  """
  msg = {'date': processing_date}
  _send_message(project, msg, topic)
  print('Message published for {} to {}'.format(msg['date'], topic))


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


def _create_key(msg):
  """Returns a key generated from the message data.

  In this case it is the message itself. It's here for extensibility purposes.

  Args:
    msg: A string representing the message

  Returns:
    A string which is the input msg parameter
  """
  return msg


def _insert_into_firestore(project, collection, msg):
  """Inserts a document into firestore.

  Args:
    project: A string representing the GCP project name
    collection: A string representing the firestore collection name
    msg: A string representing the date in process. Format YYYYMMDD
  """
  db = firestore.Client(project)
  _ = db.collection(collection).document(_create_key(msg)).set({
      'inserted_timestamp': datetime.datetime.now(pytz.utc),
      'payload': msg
  })


def _remove_from_firestore(project, collection, msg):
  """Removes a document from firestore.

  Args:
    project: A string representing the GCP project name
    collection: A string representing the firestore collection name
    msg: A string representing the date in process. Format YYYYMMDD
  """
  db = firestore.Client(project)
  _ = db.collection(collection).document(_create_key(msg)).delete()


def main(event: Dict[str, Any],
         context=Optional[google.cloud.functions.Context]):
  """Triggers the data processing corresponding to date in event.

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
  try:

    gcp_project = DEFAULT_GCP_PROJECT
    fst_collection = COLLECTION_NAME

    current_date = _get_date(msg)
    # we discard customers with more than one purchase in the same day,
    # so results will be lower than in the extract.
    _insert_into_firestore(gcp_project, fst_collection, current_date)

    input_bq_transactions_table_suffix = '{}_{}'.format(
        BQ_LTV_ALL_DAILY_TX_TABLE_ONLY, current_date)

    input_bq_transactions_table = '{}_{}'.format(BQ_LTV_ALL_DAILY_TX_TABLE,
                                                 current_date)

    output_bq_prepared_tx_data_table = '{}_{}'.format(
        BQ_LTV_PREPARED_DAILY_TX_TABLE, current_date)

    if event.get('attributes') is not None and event.get('attributes').get(
        'forwarded') is not None:

      table_check = _check_table(BQ_LTV_TABLES_METADATA,
                                 input_bq_transactions_table_suffix)
      print('Processing: {}'.format(current_date))

      if table_check == 0:  # table exists and is not empty

        metadata_df = _load_metadata(BQ_LTV_METADATA_TABLE)
        model_date = str(metadata_df['model_date'][0])
        model_name = str(metadata_df['model_name'][0])

        df = _load_tx_data_from_bq(input_bq_transactions_table)
        final_df = df
        final_df = hook_prepare(final_df, model_date)

        _write_to_bigquery(final_df, output_bq_prepared_tx_data_table)
        _send_message_to_next_stage(gcp_project, OUTBOUND_TOPIC, current_date)
      else:
        if table_check == 1:  # table exists but is empty, so do nothing
          print('Skipping: {} table is empty'.format(current_date))
        else:
          print('Skipping: {} table does not exist'.format(current_date))

      _remove_from_firestore(gcp_project, fst_collection, current_date)
    else:
      print('Throttling: {}'.format(current_date))
      _throttle_message(gcp_project, msg, ENQUEUE_TASK_TOPIC, INBOUND_TOPIC, '',
                        INBOUND_TOPIC, DELAY_IN_SECONDS)

  except:
    print('Unexpected error: {}'.format(sys.exc_info()[0]))
    _remove_from_firestore(gcp_project, fst_collection, current_date)
    raise


if __name__ == '__main__':

  msg_data = {'runTime': '2020-07-09T11:29:00Z'}
  msg_data = base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
  main(
      event={
          'data': msg_data,
          'attributes': {
              'forwarded': 'true'
          }
      },
      context=None)
