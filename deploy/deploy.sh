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
FUNCTIONS_DIR="../cfs"

source "$ENV_PATH"

function deploy_pltv_scaffolding {
  print_welcome_message
  gcloud config set project "$DEFAULT_GCP_PROJECT"
  gcloud config set compute/region "$DEFAULT_GCP_REGION"

  # Begin deploying pLTV

  enable_services
  echo "**************************************************************"
  echo "* Services Enabled. Waiting for changes to be applied...     *"
  echo "**************************************************************"
  sleep 30
  create_service_account
  echo "**************************************************************"
  echo "* Service Account Created.                                   *"
  echo "**************************************************************"
  set_service_account_permissions
  echo "**************************************************************"
  echo "* Account Permissions Set.                                   *"
  echo "**************************************************************"
  create_schedulers
  echo "**************************************************************"
  echo "* Schedulers Successfully Deployed.                          *"
  echo "**************************************************************"
  sh create_bq_elements.sh
  populate_bq
  if [[ $? -ne 0 ]]; then
    echo "Error creating BQ elements. Exiting!"
    exit -1
  fi
  sh customization/scripts/custom_deploy.sh 
  if [[ $? -ne 0 ]]; then
    echo "Error running custom deployment scripts. Exiting!"
    exit -1
  fi
  echo "**************************************************************"
  echo "* BQ Elements Successfully Deployed.                         *"
  echo "**************************************************************"
  deploy_cloud_functions
  echo "**************************************************************"
  echo "* Cloud Functions Successfully Deployed.                     *"
  echo "**************************************************************"
  echo "**************************************************************"
  echo " IMPORTANT: run the post deployment tasks explained in the doc!"
  echo " IMPORTANT: grant $SERVICE_ACCOUNT on external resources!!    "
  echo "**************************************************************"
  echo "**************************************************************"
  print_completion_message
}
# Prints the pLTV ASCII-art logo
function print_pltv_scaffolding_logo {
  echo "
        ██████╗ ██╗  ████████╗██╗   ██╗       ███████╗ ██████╗ █████╗ ███████╗███████╗ ██████╗ ██╗     ██████╗ ██╗███╗   ██╗ ██████╗         
        ██╔══██╗██║  ╚══██╔══╝██║   ██║       ██╔════╝██╔════╝██╔══██╗██╔════╝██╔════╝██╔═══██╗██║     ██╔══██╗██║████╗  ██║██╔════╝         
        ██████╔╝██║     ██║   ██║   ██║       ███████╗██║     ███████║█████╗  █████╗  ██║   ██║██║     ██║  ██║██║██╔██╗ ██║██║  ███╗        
        ██╔═══╝ ██║     ██║   ╚██╗ ██╔╝       ╚════██║██║     ██╔══██║██╔══╝  ██╔══╝  ██║   ██║██║     ██║  ██║██║██║╚██╗██║██║   ██║        
███████╗██║     ███████╗██║    ╚████╔╝███████╗███████║╚██████╗██║  ██║██║     ██║     ╚██████╔╝███████╗██████╔╝██║██║ ╚████║╚██████╔╝███████╗
╚══════╝╚═╝     ╚══════╝╚═╝     ╚═══╝ ╚══════╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝      ╚═════╝ ╚══════╝╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚══════╝"
}
# Prints the welcome message before deployment begins.
function print_welcome_message {
  print_pltv_scaffolding_logo
  echo "
Now deploying _pLTV_scaffolding_ ...
<><><><><><><><><><><><><><><><><><><>"
}
# Enable the necessary cloud services used in pLTV_scaffolding
function enable_services {
  gcloud services enable \
    appengine.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    firestore.googleapis.com \
    servicemanagement.googleapis.com \
    servicecontrol.googleapis.com \
    endpoints.googleapis.com \
    bigquery.googleapis.com \
    bigquerydatatransfer.googleapis.com \
    automl.googleapis.com \
    --format "none"
}
# Create the necessary BQ transfers.
function create_bigquery_transfers {
  echo ""
}
# Loop through the CF directories and deploy the Cloud Functions
function deploy_cloud_functions {
  for cf_dir in $FUNCTIONS_DIR/*/
  do
    pushd "$cf_dir" > /dev/null
    echo "Now deploying $cf_dir"
    sh deploy.sh
    popd > /dev/null
  done
}
function create_schedulers {
    SC_TX_POLLER=$(gcloud scheduler jobs create pubsub "$DEPLOYMENT_NAME"_"$SOLUTION_PREFIX"_periodic_transactions_poller \
    --schedule "${DATA_SOURCE_PERIODIC_TX_POLLER_CONFIG//\\/}" \
    --time-zone "$TIMEZONE" \
    --topic "$DEPLOYMENT_NAME.$SOLUTION_PREFIX.$POLLING_PERIODIC_TX_TOPIC" \
    --message-body "It's Poll Tx Time!" \
    --format "none")
    ERROR=$(echo "$SC_TX_POLLER" | grep -Po "error")
    if [[ "$ERROR" == "error" ]]; then
      EXISTS=$(echo "$SC_TX_POLLER" | grep -Po "exists")
      if [[ "$EXISTS" == "exists" ]]; then
          echo "INFO: periodic tx scheduler already exists."
      else
          echo "$SC_TX_POLLER"
          exit -1
      fi
    fi
    SC_TASK_POLLER=$(gcloud scheduler jobs create pubsub "$DEPLOYMENT_NAME"_"$SOLUTION_PREFIX"_long_running_tasks_poller \
    --schedule "${LONG_RUNNING_TASKS_POLLER_CONFIG//\\/}" \
    --time-zone "$TIMEZONE" \
    --topic "$DEPLOYMENT_NAME.$SOLUTION_PREFIX.$POLLING_LONG_RUNNING_TASKS_TOPIC" \
    --message-body "It's Poll Task Time!" \
    --format "none")
    ERROR=$(echo "$SC_TASK_POLLER" | grep -Po "error")
    if [[ "$ERROR" == "error" ]]; then
      EXISTS=$(echo "$SC_TASK_POLLER" | grep -Po "exists")
      if [[ "$EXISTS" == "exists" ]]; then
          echo "INFO: Long Running Tasks scheduler already exists."
      else
          echo "$SC_TASK_POLLER"
          exit -1
      fi
    fi
}
# Determines if the specified service account exists.
# If not, creates a new service account and gives it some necessary permissions.
function create_service_account {
  # Check whether an existing Gaudi service account exists
  CREATE_SERVICE_ACCOUNT=$(gcloud iam service-accounts list \
    --filter="email ~ $SERVICE_ACCOUNT" \
    --format="value(email)")
  echo "First Check: $CREATE_SERVICE_ACCOUNT"
  # If the client service account doesn't exist
  if [[ -z "$CREATE_SERVICE_ACCOUNT" ]] || [[ $CREATE_SERVICE_ACCOUNT == "" ]]
  then
    CLEAN_SERVICE_ACCOUNT=$(echo "$SERVICE_ACCOUNT" | sed 's/@.*//')
    echo "$CLEAN_SERVICE_ACCOUNT"
    RESULT_CREATE_SERVICE_ACCOUNT=$(gcloud iam service-accounts create "$CLEAN_SERVICE_ACCOUNT" \
      --description "Service Account for pLTV processing" \
      --display-name "pLTV Service Account" \
      --format="value(email)")
    echo "$RESULT_CREATE_SERVICE_ACCOUNT"
  fi
  if [[ "$SERVICE_ACCOUNT" = *"@"* ]]; then
      echo "Service Account Contains The Domain Already. Skipping..."
  else
      sed -i "s/SERVICE_ACCOUNT.*/SERVICE_ACCOUNT: '$CREATE_SERVICE_ACCOUNT'/" ./config.yaml
      echo "Domain Added To Service Account."
  fi
}
function set_service_account_permissions {
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/editor \
  --format "none"
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/bigquery.admin \
  --format "none"
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/automl.editor \
  --format "none"

}
function write_metadata_csv {
  echo ",$MODEL_DATE,$MODEL_ID" > ./_temp_metadata.csv
}
function populate_bq {
  write_metadata_csv
  METADATA=$(bq --project_id="$BQ_LTV_GCP_PROJECT" load  "$BQ_LTV_DATASET.$BQ_LTV_METADATA_TABLE" \
   ./_temp_metadata.csv \
   export_date:STRING,model_date:STRING,model_id:STRING)
  if [[ $? -ne 0 ]]; then
    echo "Error populating metadata BQ table. Exiting!"
    exit -1
  fi

}
# Print the completion message once pLTV has been deployed.
function print_completion_message {
  echo "
<><><><><><><><><><><><><><><><><><><>
"
  print_pltv_scaffolding_logo
  echo "$DEPLOY_NAME.$SOLUTION_NAME pLTV scaffolding has now been deployed.
Please check the logs for any errors. If there are none, you're all set up!
"
}
deploy_pltv_scaffolding
