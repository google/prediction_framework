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

"""Google Cloud function code to write messages to wait into firestore."""

import base64
import datetime
import json
import logging
import os

from typing import Any, Dict, Optional
from google.cloud import firestore
from google.cloud.functions_v1.context import Context
import google.cloud.logging
import pytz
import sys

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



COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''), os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_LONG_RUNNING_TASKS_COLLECTION', ''))
DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

DISCARD_TASKS_OLDER_THAN_HOURS = int(
    os.getenv('DISCARD_TASKS_OLDER_THAN_HOURS', '3'))


def _insert_into_firestore(project, collection, msg):
  """Writes a message into Firestore.

  Args:
    project: String representing the GCP project to use for the firestore DB
    collection: String representing the firestore collection to use
    msg: JSON object to write as Firestore document
  """

  db = firestore.Client(project)
  _ = db.collection(collection).add(msg)


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Triggers writing a message into Firestore.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context  # unused
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')

  msg = json.loads(pubsub_message)
  now = datetime.datetime.now(pytz.utc)
  msg['inserted_timestamp'] = now

  if msg.get('operation_name', None) == 'Delayed Forwarding':
    delta = datetime.timedelta(seconds=int(msg['delay_in_seconds']))
  else:
    delta = datetime.timedelta(hours=DISCARD_TASKS_OLDER_THAN_HOURS)
  msg['expiration_timestamp'] = now + delta
  msg['updated_timestamp'] = now

  logger.debug('Inserting long runnning task into Firestore. msg: %s', msg)
  _insert_into_firestore(DEFAULT_GCP_PROJECT, COLLECTION_NAME, msg)


if __name__ == '__main__':
  msg_data = {
      'status_check_url': 'https://europe-west4-aiplatform.googleapis.com/v1/projects/988912752389/locations/europe-west4/batchPredictionJobs/4741173303807311872', 'status_field': 'state', 'status_success_values': ['JOB_STATE_SUCCEEDED'], 'status_error_values': ['JOB_STATE_FAILED', 'JOB_STATE_EXPIRED'], 'payload': {'bq_input_to_predict_table': 'decent-fulcrum-316414.test.filtered_periodic_transactions', 'bq_output_table': 'decent-fulcrum-316414.test.predictions', 'date': '20210401'}, 'error_topic': 'pablogil_test.pltv.', 'success_topic': 'pablogil_test.pltv.post_process_batch_predictions', 'source_topic': 'pablogil_test.pltv.predict_transactions_batch', 'concurrent_slot_document': 'pablogil_test_pltv_prediction_tracking/concurrent_document'
  }

  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
      },
      context=None)
