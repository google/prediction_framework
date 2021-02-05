#!/bin/bash
# Tests the chunk call
#
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

CONFIG_PATH="../../deploy/config.yaml"
HELPERS_PATH="../../deploy/helpers.sh"

source "$HELPERS_PATH"
eval "$(parse_yaml $CONFIG_PATH)"

TOPIC_NAME=$PREDICT_TRANSACTIONS_TOPIC

gcloud config set project "$DEFAULT_GCP_PROJECT"
gcloud pubsub topics publish "$TOPIC_NAME" \
  --message '{"bq_input_to_predict_table": "test.ltv_ml.prepared_new_customers_periodic_transactions",
              "bq_output_table": "test.ltv_ml.predictions",
              "date": "20200501",
              "start_index": 1,
              "end_index": 6,
              "batch_size": 1000,
              "total": 21
              }'


