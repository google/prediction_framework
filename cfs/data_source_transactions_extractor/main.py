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
import datetime
import json
import os
import sys
import time

from google.api_core import datetime_helpers
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1

DEFAULT_GCP_PROJECT = str(os.getenv('DEFAULT_GCP_PROJECT', ''))

BQ_LTV_GCP_PROJECT = str(os.getenv('BQ_LTV_GCP_PROJECT', ''))

BQ_LTV_DATASET = str(os.getenv('BQ_LTV_DATASET', ''))

BQ_DATA_SOURCE_GCP_PROJECT = str(os.getenv('BQ_DATA_SOURCE_GCP_PROJECT', ''))

BQ_DATA_SOURCE_DATA_SET = str(os.getenv('BQ_DATA_SOURCE_DATA_SET', ''))

BQ_DATA_SOURCE_TABLES = str(os.getenv('BQ_DATA_SOURCE_TABLES',
                                      ''))  # 'ga_sessions_*'

BQ_LTV_TRANSFER_PROJECT_ID = str(os.getenv('BQ_LTV_TRANSFER_PROJECT_ID', ''))

BQ_LTV_PERIODIC_TX_TRANSFER_ID = str(os.getenv('BQ_LTV_PERIODIC_TX_TRANSFER_ID', ''))

BQ_LTV_TRANSFER_REGION = str(os.getenv('BQ_LTV_TRANSFER_REGION', ''))

BQ_LTV_ALL_PERIODIC_TX_TABLE = str(os.getenv('BQ_LTV_ALL_PERIODIC_TX_TABLE', ''))

BQ_DATA_SOURCE_TX_TABLE_PREFIX = '{}.{}.{}_*'.format(BQ_DATA_SOURCE_GCP_PROJECT,
                                                     BQ_DATA_SOURCE_DATA_SET,
                                                     BQ_DATA_SOURCE_TABLES)

BQ_LTV_INTERMEDIATE_TX_TABLE_PREFIX = '{}.{}.{}_*'.format(
    BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET, BQ_LTV_ALL_PERIODIC_TX_TABLE)


def _is_number(s):
  """Returns if the input string is a number.

  Args:
    s: A String.

  Returns:
    True or False

    example: True
  """
  try:
    float(s)
    return True
  except ValueError:
    return False


def _get_date(timestamp):
  """Returns the rfc339 included into the event content and the day before.

  Args:
    timestamp: The event generetad by the pub/sub trigger.

  Returns:
    An array of 2 elements being the first one the date included into the event
    and the second element the day before
  """
  date = datetime_helpers.from_rfc3339(timestamp)

  days = datetime.timedelta(1)
  day_before = date - days

  return date, day_before


def _check_data_source_table(data_source_project, data_source_data_set,
                             ltv_project, ltv_dataset, input_table,
                             intermediate_table, current_date, day_before):
  """Checks if the data source table is available & no extract table generated.


  Args:
    data_source_project: the string representing the data source gcp project
    data_source_data_set: the string representing the data source bq dataset
    ltv_project: the string representing the ltv gcp project
    ltv_dataset: the string representing the ltv bq data set
    input_table: the string representing the input table
    intermediate_table: the string representing the intermediate table
    current_date: the date representing the day to process
    day_before: the date representing the day before to the one to process

  Returns:
    A pandas dataframe containing the status of the input BQ table
    example:

    df['result'][0]==0: data source exists and intermediate has not been created
                        yet
    df['result'][0]==1: both data source and intermediate table exists
    df['result'][0]==2: data source table does not exist
  """
  query = f"""
      if exists(SELECT size_bytes FROM
        `{data_source_project}.{data_source_data_set}.__TABLES__`
      where table_id = "{input_table}_{day_before}") then
        if (exists(
            SELECT size_bytes FROM `{ltv_project}.{ltv_dataset}.__TABLES__`
            where  table_id = "{intermediate_table}_{current_date}")) then
          select 1 as result;
        else
          select 0 as result;
        end if;
      else
        select 2 as result;
      end if;
     """

  return bigquery.Client().query(query).to_dataframe().reset_index(drop=True)


def _extract_periodic_transactions(transfer_project_id, transfer_id, region,
                                seconds):
  """Executes the data transfer.

  Args:
    transfer_project_id: string representing the bq data transfer project
    transfer_id: string representing the bq data transfer
    region: string representing the region for the BQ data transfer
    seconds: number of seconds representing the processing date
  """
  try:
    client = bigquery_datatransfer_v1.DataTransferServiceClient()

    parent = client.location_transfer_config_path(transfer_project_id, region,
                                                  transfer_id)
    start_time = bigquery_datatransfer_v1.types.Timestamp(seconds=seconds)
    response = client.start_manual_transfer_runs(
        parent, requested_run_time=start_time, timeout=300.00)
    print('Executing transfer_id: {} run_id: {}'.format(
        parent, str(response.runs[0].name)))
  # pylint: disable=broad-except
  except Exception:
    print('Exception {} occurred'.format(sys.exc_info()[0]))
  # pylint: enable=broad-except


def _is_backfill(event):
  """Checks if the event corresponds to a backfill request.

  Args:
    event: the event generated by pub/sub trigger

  Returns:
    True if message contains backfill attribute
    False otherwise
  """
  return event.get('attributes') is not None and event.get('attributes').get(
      'backfill') is not None


def main(event, context):
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

  if _is_backfill(event):
    body = base64.b64decode(event['data']).decode('utf-8')
    j = json.loads(body)
    publish_d, d_before = _get_date(j['timestamp'])
  else:
    publish_d, d_before = _get_date(context.timestamp)

  publish_date = publish_d.strftime('%Y%m%d')
  day_before = d_before.strftime('%Y%m%d')

  print('Processing ', publish_date)

  df = _check_data_source_table(BQ_DATA_SOURCE_GCP_PROJECT,
                                BQ_DATA_SOURCE_DATA_SET, BQ_LTV_GCP_PROJECT,
                                BQ_LTV_DATASET, BQ_DATA_SOURCE_TABLES,
                                BQ_LTV_ALL_PERIODIC_TX_TABLE, publish_date,
                                day_before)

  if not df['result'][0]:
    _extract_periodic_transactions(BQ_LTV_TRANSFER_PROJECT_ID,
                                BQ_LTV_PERIODIC_TX_TRANSFER_ID,
                                BQ_LTV_TRANSFER_REGION,
                                int(time.mktime(publish_d.timetuple())))


def _backfill():
  """Test call to process specific date."""
  data = {'timestamp': '2020-09-02T15:01:23.045123456Z'}
  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(data).encode('utf-8'))),
          'attributes': {
              'backfill': 'true'
          }
      },
      context=None)


def _normal_call():
  """Test call to process current date."""
  data = {}

  main(
      event={'data': base64.b64encode(bytes(json.dumps(data).encode('utf-8')))},
      context=None)


if __name__ == '__main__':
  _backfill()
