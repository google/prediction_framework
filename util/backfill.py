"""The module executes manual backfilling, if not possible to do it via BQ UI."""

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
import datetime
import json
from typing import List

from absl import app
from google.api_core import datetime_helpers
from google.cloud import pubsub_v1
import pytz

from google3.pyglib import datelib


def _custom_type_formatter(o):
  """Stringifies a datetime object so it can be publish on a pub/sub.

  Args:
    o: any object

  Returns:
    A string representing the date if the object is of type datetime
    None otherwise
  """
  if isinstance(o, datetime.datetime):
    return o.__str__()


def _publish_message(project, topic_name, run_date):
  """Sends 1 message per each day between the 2 inputs dates specified in argv.

  Args:
    project: a string representing the GCP project to use
    topic_name: a string representing the topic name
    run_date: a string representing the date to process in YYYYMMDD format
  """

  msg = {'timestamp': run_date}
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic_name)
  json_str = json.dumps(msg, default=_custom_type_formatter)

  _ = publisher.publish(
      topic_path, data=bytes(json_str, 'utf-8'), backfill='1').result()


def main(argv: List[str]) -> None:
  """Sends 1 message per each day between the 2 inputs dates specified in argv.

  Args:
    argv: a string array representing the CLI parameters, start date and end
      date in YYYYMMDD format
  """

  if len(argv) < 3:
    raise app.UsageError("""Too few command-line arguments.
    Execute python backfill.py <start_date> <end_date>.
    Example of use: python backfill.py 20200827 20200918.""")
  else:
    print('Executing with parameters ', argv)

    project = argv[1]
    topic = argv[2]
    start = argv[3]
    end = argv[4]

    sdate = datelib.CreateDatetime(
        year=int(start[0:4]),
        month=int(start[4:6]),
        day=int(start[6:8]),
        tzinfo=pytz.UTC)

    edate = datelib.CreateDatetime(
        year=int(end[0:4]),
        month=int(end[4:6]),
        day=int(end[6:8]),
        tzinfo=pytz.UTC)

    delta = edate - sdate

    for i in range(delta.days):
      day = sdate + datetime.timedelta(days=i)
      day = datetime_helpers.to_rfc3339(day, ignore_zone=True)
      print('Executing day ', day)
      _publish_message(project, topic, day)


if __name__ == '__main__':
  app.run(main)
