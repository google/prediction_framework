#!/bin/bash
# Tests enqueuing the task
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

TOPIC_NAME=$PREDICT_TRANSACTIONS_TOPIC

gcloud config set project "$DEFAULT_GCP_PROJECT"
gcloud pubsub topics publish "$TOPIC_NAME" \
  --message '{
   "client_class_module":"google.cloud.automl_v1beta1",
   "operation_name":"projects/fiz/locations/eu/operations/buzz",
   "source_topic":"test.pltv.predict_transactions",
   "error_topic":"",
   "source_payload":{
      "bq_input_to_predict_table":"test.ltv_ml.prepared_new_customers_periodic_transactions",
      "bq_output_table":"test.ltv_ml.predictions",
      "date":"20200519"
   },
   "client_class":"AutoMlClient",
   "inserted_timestamp":"2020-07-23 15:02:01.853163+00:00",
   "expiration_timestamp":"2020-07-23 18:02:01.853183+00:00",
   "success_topic":"test.pltv.predict_transactions",
   "payload":{
      "bq_input_to_predict_table":"test.ltv_ml.prepared_new_customers_periodic_transactions",
      "bq_output_table":"test.ltv_ml.predictions",
      "date":"20200824"
   },
   "client_params":{
      "client_options":{
         "api_endpoint":"eu-automl.googleapis.com:443"
      }
   }
}'



