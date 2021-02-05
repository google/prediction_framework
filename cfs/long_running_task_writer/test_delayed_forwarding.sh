#!/bin/bash
# Performs test of delayed forwarding message.
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
   "payload":{
      "runTime":"2020-06-20T02:00:00Z"
   },
   "operation_name":"Delayed Forwarding",
   "delay_in_seconds":120,
   "error_topic":"",
   "success_topic":"test.pltv.periodic_extract_ready",
   "source_topic":"test.pltv.periodic_extract_ready"
}'
