#!/bin/bash
# Performs the BQ elements creation
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
CONFIG_PATH="./config.yaml"
QUERY_PATH="customization/queries/extract_all_transactions.sql"

source "$ENV_PATH"

if [[ "$SKIP_LTV_DATASET_CREATION" == "N" ]]; then
  CREATE_LTV_DATASET=$(bq mk -d \
  --location "$BQ_LTV_GCP_BROAD_REGION" \
  "$BQ_LTV_GCP_PROJECT:$BQ_LTV_DATASET"
  )
  ERROR=$(echo "$CREATE_LTV_DATASET" | grep -Po "error")
  #echo $ERROR
  if [[ "$ERROR" == "error" ]]; then
    EXISTS=$(echo "$CREATE_LTV_DATASET" | grep -Po "exists")
    if [[ "$EXISTS" == "exists" ]]; then
        echo "INFO: Dataset $BQ_LTV_DATASET Already exists."
    else
        echo "$CREATE_LTV_DATASET"
        exit -1
    fi
  else
    echo "***************************************************************"
    echo "* BQ Dataset Successfully Deployed. Waiting to be available.  *"
    echo "***************************************************************"
    sleep 30
  fi
fi

TABLE="$BQ_DATA_SOURCE_GCP_PROJECT"".""$BQ_DATA_SOURCE_DATA_SET"".""$BQ_DATA_SOURCE_TABLES""_*"

echo "$TABLE"


QUERY=$(cat "$QUERY_PATH")
QUERY=$(echo "$QUERY" | sed -r 's,\\[trn],,g')
QUERY=$(echo "$QUERY" | sed -r 's,\\,\\\\,g')
QUERY=$(echo "$QUERY" | sed -r 's,\",\\",g')
QUERY=$(echo "$QUERY" | sed -r ':a;N;$!ba;s/\n/\\n/g')
QUERY=$(echo "$QUERY" | sed -r 's,\$TABLE,'"$TABLE"',g')
echo "$QUERY"

#echo $QUERY
TARGET_TABLE_TEMPLATE="$BQ_LTV_ALL_PERIODIC_TX_TABLE"'_{run_time|\"%Y%m%d\"}'
PARAMS='{"query":"'$QUERY'","destination_table_name_template" :"'$TARGET_TABLE_TEMPLATE'","write_disposition" : "WRITE_TRUNCATE"}'
#echo $PARAMS
S="bq mk \
--transfer_config \
--location="$BQ_LTV_GCP_BROAD_REGION" \
--project_id="$BQ_LTV_GCP_PROJECT" \
--target_dataset="$BQ_LTV_DATASET" \
--display_name="$DEPLOYMENT_NAME""_""$SOLUTION_PREFIX""_extract_all_transactions" \
--data_source=scheduled_query \
--schedule='None' \
--service_account_name="$SERVICE_ACCOUNT" \
--params=\"$PARAMS\" "

echo "$S"

CREATE_TRANSFER=$(bq mk \
--transfer_config \
--location="$BQ_LTV_GCP_BROAD_REGION" \
--project_id="$BQ_LTV_GCP_PROJECT" \
--target_dataset="$BQ_LTV_DATASET" \
--display_name="$DEPLOYMENT_NAME""_""$SOLUTION_PREFIX""_extract_all_transactions" \
--data_source=scheduled_query \
--schedule='None' \
--service_account_name="$SERVICE_ACCOUNT" \
--params="$PARAMS"
)

ERROR=$(echo "$CREATE_TRANSFER" | grep -Po "error")
if [[ "$ERROR" == "error" ]]; then
    echo "$CREATE_TRANSFER"
    exit -1
fi
echo "$CREATE_TRANSFER"
PROJECT_ID=$(echo "$CREATE_TRANSFER" | grep -Po "projects/\K[^/locations]*")
TRANSFER_ID=$(echo "$CREATE_TRANSFER" | grep -Po "transferConfigs/\K[^']*")
echo "$PROJECT_ID"
echo "$TRANSFER_ID"
sed -i "s/BQ_LTV_TRANSFER_PROJECT_ID.*/BQ_LTV_TRANSFER_PROJECT_ID: '$PROJECT_ID'/" "$CONFIG_PATH"
sed -i "s/BQ_LTV_PERIODIC_TX_TRANSFER_ID.*/BQ_LTV_PERIODIC_TX_TRANSFER_ID: '$TRANSFER_ID'/" "$CONFIG_PATH"