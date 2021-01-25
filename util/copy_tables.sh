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

ENV_PATH="./env.sh"
pushd ../deploy/ > /dev/null
source "$ENV_PATH"

echo "$1"
echo "$2"

SOURCE_DATASET="$BQ_LTV_GCP_PROJECT:$BQ_LTV_DATASET"
MAX_TABLES=100

echo "bq ls -n $MAX_TABLES $SOURCE_DATASET"
for t in $(bq ls -n "$1" "$SOURCE_DATASET" | grep TABLE | grep "$2" | awk '{print $1}')
do
  CMD="bq cp -f $SOURCE_DATASET.$t $3.$t"
  echo "$CMD"
  RES=$("$CMD")
  echo "$RES"
  sleep 3
done
