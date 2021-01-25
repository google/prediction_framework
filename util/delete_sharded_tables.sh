#!/bin/bash
# Performs cloud function deployment
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

gcloud config set project "$DEFAULT_GCP_PROJECT"
gcloud config set compute/region "$DEFAULT_GCP_REGION"

for i in $(bq ls -n 9999 "$1" | grep "$2" | awk '{print $1}'); do bq rm "$3" -t "$1"."$i";done;
