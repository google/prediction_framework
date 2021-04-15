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
import logging
import os
import sys

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import firestore_v1 as firestore
import google.cloud.logging
from google.cloud import pubsub_v1
from google.protobuf import json_format
import pytz

# Set-up logging
client = google.cloud.logging.Client()
handler = google.cloud.logging.handlers.CloudLoggingHandler(client)
logger = logging.getLogger('cloudLogger')
logger.setLevel(logging.DEBUG) # defaults to WARN
logger.addHandler(handler)

COLLECTION_NAME = '{}_{}_{}'.format(
    os.getenv('DEPLOYMENT_NAME', ''),
    os.getenv('SOLUTION_PREFIX', ''),
    os.getenv('FST_LONG_RUNNING_TASKS_COLLECTION', ''))

DEFAULT_GCP_PROJECT = os.getenv('DEFAULT_GCP_PROJECT', '')

MAX_TASKS_PER_POLL = int(os.getenv('MAX_TASKS_PER_POLL', '5'))
MAX_TASKS_PER_POLL_MULTIPLIER = 10


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

  logger.info('Forwarded to: %s', topic)


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
                max_tasks,
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

def _decrease_counter(fs_db, d_task):
  """Decrease the counter.

  Args:
    fs_db: Firestore db
    task: Dictionary of the firestore document to be deleted
  """  
  try:
    concurrent_slot_document = d_task.get('concurrent_slot_document', None)
    if concurrent_slot_document:
      logger.info('Decreasing concurrent counter. Document: %s',
                   concurrent_slot_document)
      transaction = fs_db.transaction()
      doc_ref = firestore.document.DocumentReference(*concurrent_slot_document.split('/'))
      _release_concurrent_slot(transaction, doc_ref)
    else:
      logger.debug('Concurrent slot document not found. Nothing to do.')
  except Exception:
    logger.exception('Exception while decreasing counter')
    pass



def _delete_task(project, collection, task):
  """Deletes a document from the firestore collection.

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task: The firestore document to be deleted
  """
  db = firestore.Client(project)
  _decrease_counter(db, task.to_dict())
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


def _process_tasks(project, collection, task_list, max_tasks, current_date_time):
  """Iterates over the task_list to process each task.

  Args:
    project: String representing the GCP project to use
    collection: A string representing the firestore collection
    task_list: A firestore documents array to be processed
    max_tasks: Integer representing the maximum "in window" tasks to process
    current_date_time: A datetime object representing the current date & time
  """
  counter = 0
  for task in task_list:
    try:
      counter += _process_task(project, collection, task, current_date_time)
      if counter >= max_tasks:
        break
    # in this case we need to capture all the exceptions to send the task to
    # the error queue, whatever the exception. That's why bare except.
    # pylint: disable=bare-except
    except:
      logger.exception('Exception while processing task: %s', task)
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


@firestore.transactional
def _release_concurrent_slot(transaction, doc_ref):
  snapshot = doc_ref.get(transaction=transaction)
  new_count = snapshot.get('concurrent_count') - 1
  transaction.update(doc_ref, {'concurrent_count': new_count})


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

  Returns:
    1 if the task was not expired and ("in window" or long running operation is done)
    , 0 in any other case

  """

  d_task = task.to_dict()

  if _it_is_expired(d_task, current_date_time):
    logger.info('Task expired: %s', d_task)
    _send_to_error(project, d_task)
    _delete_task(project, collection, task)
  else:
    operation_name = d_task['operation_name']
    if operation_name == 'Delayed Forwarding':
      logger.info('Delayed forwarding: %s', d_task)
      if _it_is_time(d_task, current_date_time):
        _send_to_success(project, d_task)
        _delete_task(project, collection, task)
        return 1
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
        logger.info('Task done: %s', d_task)
        _decrease_counter(firestore.Client(project=DEFAULT_GCP_PROJECT), d_task)
        if hasattr(op, 'response'):
          d_task['payload']['operation'] = json_format.MessageToDict(op)
          _send_to_success(project, d_task)
        else:
          _send_to_error(project, d_task)

        _delete_task(project, collection, task)
        return 1
      else:
        logger.debug('Task not done yet: %s', d_task)
        _update_task_timestamp(project, collection, task)
    return 0


def main(event: Dict[str, Any], context=Optional[Context]):
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
  num_tasks = MAX_TASKS_PER_POLL * MAX_TASKS_PER_POLL_MULTIPLIER

  task_list = _load_tasks(gcp_project, collection, num_tasks, current_date_time)
  _process_tasks(gcp_project, collection, task_list, MAX_TASKS_PER_POLL, current_date_time)


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


def _create_data():
  body = {
      'inserted_timestamp':
          datetime.datetime.now(),
      'source_topic':
          'pltv_fwk.jaimemm_tests.predict_transactions_batch',
      'client_class':
          'AutoMlClient',
      'client_class_module':
          'google.cloud.automl_v1beta1',
      'client_params': {
          'client_options': {
              'api_endpoint': 'eu-automl.googleapis.com:443'
          }
      },
      'concurrent_slot_document': 'prediction_tracking/concurrent_document',
      'success_topic':
          'pltv_fwk.jaimemm_tests.copy_batch_predictions',
      'operation_name':
          'projects/988912752389/locations/eu/operations/TBL8979557532418703360',
      'error_topic':
          '',
      'expiration_timestamp':
          datetime.datetime.now(),
      'payload': {
          'bq_input_to_predict_table':
              'ltv-framework.ltv_jaimemm.prepared_new_customers_periodic_transactions',
          'bq_output_table':
              'ltv-framework',
          'date':
              '20210304'
      },
      'updated_timestamp':
          datetime.datetime.now()
  }

  db = firestore.Client(project=DEFAULT_GCP_PROJECT)
  db.collection(COLLECTION_NAME).add(body)
  main(event=None, context=None)


if __name__ == '__main__':
  _create_data()
