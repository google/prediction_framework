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
SCRIPTS_PATH="customization/scripts"

source "$ENV_PATH"
chmod -R 777 "$SCRIPTS_PATH"
# Include your custom script call here
# ie.
# customization/scripts/sample_custom_features_data_script.sh
# customization/scripts/sample_custom_values_script.sh
