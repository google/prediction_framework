"""This module is useful to clean the firestore tables during the tests.

Thousands of entries could be generated and it won't be possible to remove them
from the Firestore UI
"""

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

from typing import List

from absl import app
from google.cloud import firestore


def _delete_collection(coll_ref, batch_size):
  """Deletes the collection documents in batches of batch_size.

  Args:
    coll_ref: a string array representing the CLI parameters, start date and end
      date in YYYYMMDD format
    batch_size: maximum amount of tables to delete in a row

  Returns:
    Itself if not all deleted
    None otherwise
  """
  docs = coll_ref.limit(batch_size).stream()
  deleted = 0

  for doc in docs:
    doc.reference.delete()
    deleted = deleted + 1
  print('Items deleted ', deleted)
  if deleted >= batch_size:
    return _delete_collection(coll_ref, batch_size)


def main(argv: List[str]) -> None:
  """Deletes firestore collections between 2 dates.

  Args:
    argv: an array representing the input parameters:
      argv[1]: a string array representing the GCP project to use
      argv[2]: a string representing the name of the collection
      argv[3]: an integer representing the start date in YYYYMMDD format
      argv[4]: an integer representing the end date in YYYYMMDD format
  """
  if len(argv) < 3:
    raise app.UsageError('Too few command-line arguments.')
  else:
    print('Arguments ', argv)
    db = firestore.Client(project=argv[1])
    collection_name = argv[2]
    if len(argv) > 3:

      start = int(argv[3])
      end = int(argv[4])
      while start <= end:
        collection_name = '{}_{}'.format(argv[2], start)
        collection = db.collection(collection_name)
        _delete_collection(collection, 1000)
        start = start + 1
    else:
      collection = db.collection(collection_name)
      _delete_collection(collection, 20)


if __name__ == '__main__':
  app.run(main)
