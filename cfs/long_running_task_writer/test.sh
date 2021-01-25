#!/bin/bash
# Performs test of normal message scenario.
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


TOPIC_NAME="$ENQUEUE_TASK_TOPIC"

gcloud config set project "$DEFAULT_GCP_PROJECT"
gcloud pubsub topics publish "$TOPIC_NAME" \
  --message '{
   "operation_name":"projects/YYYYYYY/locations/eu/operations/TBLZZZZZ",
   "client_class_module":"google.cloud.automl_v1beta1",
   "client_class":"AutoMlClient",
   "client_params":{
      "client_options":{
         "api_endpoint":"eu-automl.googleapis.com:443"
      }
   },
   "source_payload":{
      "bq_input_uri":"test.ltv_ml.prepared_new_customers_daily_transactions_20200710",
      "bq_output_predict_prefix":"xxxxx.ltv_ml.predictions",
      "date":"20200710",
      "model_gcp_project":"test",
      "model_name":"training_data_20200605_0608",
      "model_region":"eu",
      "model_api_endpoint":"eu-automl.googleapis.com:443"
   },
   "payload":{
      "bq_input_uri":"test.ltv_ml.prepared_new_customers_daily_transactions_20200710",
      "bq_output_predict_prefix":"test.ltv_ml.predictions",
      "date":"20200710",
      "model_gcp_project":"test",
      "model_name":"training_data_20200605_0608",
      "model_region":"eu",
      "model_api_endpoint":"eu-automl.googleapis.com:443"
   },
   "error_topic":"test_error",
   "success_topic":"test_success",
   "source_topic":"source_topic"
}'
