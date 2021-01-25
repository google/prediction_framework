# it's executed from deploy path
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

# i.e:
#
# source ./env.sh
# bq --project_id=$BQ_LTV_GCP_PROJECT load \
# --replace=true $BQ_LTV_DATASET"."$BQ_LTV_TRAINING_DATA_PRICE_AVGS_TABLE"_"$MODEL_DATE \
# ./customization/data/sample_training_data_features.csv \
# value:STRING,type:STRING
# if [[ $? -ne 0 ]]; then
#  echo "Error populating features BQ table. Exiting!"
#  exit -1
# fi

