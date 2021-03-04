"""Google Cloud function code to write messages to wait into firestore."""

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
from google.cloud import firestore
from google.cloud.functions_v1.context import Context
import pytz

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

  if msg['operation_name'] == 'Delayed Forwarding':
    delta = datetime.timedelta(seconds=int(msg['delay_in_seconds']))
  else:
    delta = datetime.timedelta(hours=DISCARD_TASKS_OLDER_THAN_HOURS)
  msg['expiration_timestamp'] = now + delta
  msg['updated_timestamp'] = now

  _insert_into_firestore(DEFAULT_GCP_PROJECT, COLLECTION_NAME, msg)


if __name__ == '__main__':
  msg_data = {
      'payload': {
          'runTime': '2020-06-20T02:00:00Z'
      },
      'operation_name': 'Delayed Forwarding',
      'delay_in_seconds': 120,
      'error_topic': '',
      'success_topic': 'test.pltv.periodic_extract_ready',
      'source_topic': 'test.pltv.periodic_extract_ready'
  }

  main(
      event={
          'data': base64.b64encode(bytes(json.dumps(msg_data).encode('utf-8')))
      },
      context=None)
