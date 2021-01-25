#!/bin/bash
# Tests the sream prediction flow
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
#

CONFIG_PATH="../../deploy/config.yaml"
HELPERS_PATH="../../deploy/helpers.sh"

source "$HELPERS_PATH"
eval "$(parse_yaml $CONFIG_PATH)"

TOPIC_NAME="$PREDICT_TRANSACTION_TOPIC"

gcloud config set project "$DEFAULT_GCP_PROJECT"
gcloud pubsub topics publish "$TOPIC_NAME" \
  --message '{
   "uuid":"343af019-1615-47dc-b4c2-6ee17b5c9dde",
   "row":{"your-row-must-be-here":"your-row-must-be-here"
   },
   "model_project":"test",
   "model_region":"eu",
   "model_api_endpoint":"eu-automl.googleapis.com:443",
   "model_name":"training_data_20200605_0608",
   "model_date":"20200605",
   "bq_output_table":"test.ltv_ml.predictions_20200710"
}'





