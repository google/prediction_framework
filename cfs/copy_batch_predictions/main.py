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
import json
import os

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import bigquery


BQ_LTV_GCP_PROJECT = str(os.getenv('BQ_LTV_GCP_PROJECT', ''))

BQ_LTV_DATASET = str(os.getenv('BQ_LTV_DATASET', ''))



def main(event: Dict[str, Any],
         context=Optional[Context]):
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
  del context

  data = base64.b64decode(event['data']).decode('utf-8')
  msg = json.loads(data)

  output_dataset = (msg['operation']['metadata']
      ['batch_predict_details']['output_info']['bigquery_output_dataset'])
  date = msg['date']

  client = bigquery.client.Client(project=DEFAULT_GCP_PROJECT)
  job = client.copy_table(f'{output_dataset}.predictions',
      f'{BQ_LTV_GCP_PROJECT}.{BQ_LTV_DATASET}.predictions_{date}')
  job.result()

