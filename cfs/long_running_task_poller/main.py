"""Google Cloud function code to for long running tasks polling."""

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
import importlib
import json
import os
import sys

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import firestore
from google.cloud import pubsub_v1
import pytz

COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''), os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_LONG_RUNNING_TASKS_COLLECTION', ''))

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

MAX_TASKS_PER_POLL = int(os.getenv('MAX_TASKS_PER_POLL', '5'))


def _send_message(project, msg, topic):
  """Sends a pub/sub message to the specified topic.

  Args:
    project: String representing the GCP project to use
    msg: JSON object to publish
    topic: The topic to publish the message to
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic)
  msg_json = json.dumps(msg['payload'])

  publisher.publish(
      topic_path, data=bytes(msg_json, 'utf-8'), forwarded='1').result()

  print('Forwarded to ', topic)


def _send_to_error(project, task):
  """Sends a pub/sub message to the error topic defined in the task.

  Args:
    project: String representing the GCP project to use
    task: JSON object to publish
  """
  _send_message(project, task, task['error_topic'])


def _send_to_success(project, task):
  """Sends a pub/sub message to the success topic defined in the task.

  Args:
    project: String representing the GCP project to use
    task: JSON object to publish
  """
  _send_message(project, task, task['success_topic'])


def _load_tasks(project,
                collection,
                max_tasks=MAX_TASKS_PER_POLL,
                unused_current_date_time=None):
  """Retrieves a number of documents from the firestore collection.

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    max_tasks: An integer representing the maximum number of tasks to be
      retrieved
    unused_current_date_time: A datetime object representing the current date &
      time

  Returns:
    An arrayof lists.
  """
  db = firestore.Client(project)

  return db.collection(collection).order_by(
      'expiration_timestamp',
      direction=firestore.Query.ASCENDING).limit(max_tasks).stream()


def _delete_task(project, collection, task):
  """Deletes a document from the firestore collection .

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task: The firestore document to be deleted
  """
  db = firestore.Client(project)
  db.collection(collection).document(task.id).delete()


def _update_task_timestamp(project, collection, task):
  """Updates a document in the firestore collection .

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task: The firestore document to be updated
  """
  db = firestore.Client(project)
  db.collection(collection).document(task.id).update(
      {'updated_timestamp': datetime.datetime.now(pytz.utc)})


def _process_tasks(project, collection, task_list, current_date_time):
  """Iterates over the task_list to process each task.

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task_list: A firestore documents array to be processed
    current_date_time: A datetime object representing the current date & time
  """
  for task in task_list:
    try:
      _process_task(project, collection, task, current_date_time)

    # in this case we need to capture all the exceptions to send the task to
    # the error queue, whatever the exception. That's why bare except.
    # pylint: disable=bare-except
    except:

      print('Unexpected error:', sys.exc_info()[0])

      print(f'{task.id} => {task.to_dict()}')

      try:
        _send_to_error(project, task.to_dict())
      finally:
        _delete_task(project, collection, task)
    # pylint: enable=bare-except


def _it_is_expired(task, current_date_time):
  """Checks if the task expiration time is greater than current_date_time.

  Args:
    task: A firestore document
    current_date_time: A datetime object representing the current date & time

  Returns:
    expiration_timestamp > current_date_time;

  """

  if task['operation_name'] != 'Delayed Forwarding':
    return task['expiration_timestamp'] < current_date_time
  else:
    return False


def _it_is_time(task, current_date_time):
  """Checks if the task expiration time is lower or equal than current_date_time.

  Args:
    task: A firestore document
    current_date_time: A datetime object representing the current date & time

  Returns:
    expiration_timestamp <= current_date_time;

  """
  return task['expiration_timestamp'] <= current_date_time


def _process_task(project, collection, task, current_date_time):
  """Checks if it's time to process the task.

  Depending on the type of tasks it the processing will differ:

  - Delayed Forwarding: send to success topic if it's time,
  updatime time otherwise
  - Long Running Task (otherwise): check status of long running task using the
  right API client, and send to success if completed, to error if failed or
  update the updated timestamp if not yet completed.

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task: A firestore document to be processed
    current_date_time: A datetime object representing the current date & time
  """

  d_task = task.to_dict()
  if _it_is_expired(d_task, current_date_time):
    _send_to_error(project, d_task)
    _delete_task(project, collection, task)
  else:
    operation_name = d_task['operation_name']

    if operation_name == 'Delayed Forwarding':
      if _it_is_time(d_task, current_date_time):
        _send_to_success(project, d_task)
        _delete_task(project, collection, task)
      else:
        _update_task_timestamp(project, collection, task)
    else:
      module = importlib.import_module(d_task['client_class_module'])
      class_ = getattr(module, d_task['client_class'])
      instance = class_(**d_task['client_params'])

      # pylint: disable=protected-access
      op = instance.transport._operations_client.get_operation(operation_name)
      # pylint: enable=protected-access
      if op.done:
        if hasattr(op, 'response'):
          d_task['operation_reponse'] = operation.response
          _send_to_success(project, d_task)
        else:
          _send_to_error(project, d_task)
        _delete_task(project, collection, task)
      else:
        _update_task_timestamp(project, collection, task)


def main(event: Dict[str, Any],
         context=Optional[Context]):
  """Triggers the enqueued tasks processing.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context  # unused argument
  del event  # unused argument
  current_date_time = datetime.datetime.now(pytz.utc)

  gcp_project = DEFAULT_GCP_PROJECT
  collection = COLLECTION_NAME
  max_tasks = MAX_TASKS_PER_POLL

  task_list = _load_tasks(gcp_project, collection, max_tasks, current_date_time)

  _process_tasks(gcp_project, collection, task_list, current_date_time)


def _create_test_data(event):
  """Creates dummy data in Firestore, for testing purposes.

  Args:
    event: Pub/Sub event object
  """

  db = firestore.Client(project=DEFAULT_GCP_PROJECT)
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  body = json.loads(pubsub_message)

  body['inserted_timestamp'] = datetime.datetime.now()

  unused_doc_ref = db.collection(COLLECTION_NAME).add(body)
  body['inserted_timestamp'] = datetime.datetime.now()

  unused_doc_ref = db.collection(COLLECTION_NAME).add(body)
  body['inserted_timestamp'] = datetime.datetime.now()
  unused_doc_ref = db.collection(COLLECTION_NAME).add(body)


if __name__ == '__main__':
  main(event=None, context=None)
