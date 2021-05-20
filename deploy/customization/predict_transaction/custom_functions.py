"""This module is used by the predict_transaction cloud functions."""

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
# -*- coding: utf-8 -*-

"""The aim is to calculate the final value of the prediction and define the schema
to write those predictions into BQ


 Add your imports here i.e

 import os
 from google.cloud import bigquery
 from datetime import datetime

 Declare the module global variables here i.e

 FORMULA_PREDICTION_MULTIPLIER = float(
     os.getenv('FORMULA_PREDICTION_MULTIPLIER', 0.0))
"""
from typing import List, Dict, Any
from google.cloud.automl_v1.types import PredictResponse

def hook_get_bq_schema() -> str:
  """Returns the schema of the prediction table to be written in BQ.

  Returns:
    An array of bigquery.SchemaField

      Example:

        return [
         bigquery.SchemaField('clientId','STRING',mode='REQUIRED')
        ,bigquery.SchemaField('orderId','STRING',mode='REQUIRED')
        ,bigquery.SchemaField('gclid','STRING',mode='REQUIRED')
        ,bigquery.SchemaField('date','STRING',mode='REQUIRED')
        ,bigquery.SchemaField('conversionValue','FLOAT64',mode='REQUIRED')
        ,bigquery.SchemaField('conversionValue2','FLOAT64',mode='REQUIRED')
        ,bigquery.SchemaField('conversionValue3','FLOAT64',mode='REQUIRED')
        ,bigquery.SchemaField('conversionValue4','FLOAT64',mode='REQUIRED')
        ,bigquery.SchemaField('conversionValue5','FLOAT64',mode='REQUIRED')
        ]
  """
  return []


def hook_apply_formulas(
    original_data: Dict[str, Any],
    predictions: PredictResponse) -> List[Any]:
  """Applies as many formulas as required to the prediction values.

  Args:
    original_data: An array representing the BQ row that was sent to prediction.
    predictions: The AutoML array response containing the prediction array data.

  Returns:
    An array containing the values of the predictions after applying the
    formulas.

      Example:

      If we use  classification and class A represents low value
      class B represents high value

      real_value = original_data["cartRevenue"]
      pred_a = 0.0
      pred_b = 0.0
      if (predictions.payload[0].tables.value.string_value == "A"):
          pred_a = predictions.payload[0].tables.score
          pred_b = predictions.payload[1].tables.score
      else:
          pred_a = predictions.payload[1].tables.score
          pred_b = predictions.payload[0].tables.score

      return [apply_formula(pred_a, pred_b, real_value),
              apply_formula2(pred_a, pred_b, real_value),
              apply_formula3(pred_a, pred_b, real_value),
              apply_formula4(pred_a, pred_b, real_value),
              apply_formula5(pred_a, pred_b, real_value)]

  """

  del original_data, predictions  # Unused by default

  return []


def hook_create_row_to_insert(original_data: Dict[str, Any],
                              predicted_values: List[Any]) -> Dict[str, Any]:
  """Builds a JSON object based on the predicted values.

  It considers the values after applying the formulas and returns the full row
  object which will be inserted into BQ later.

  Args:
    original_data: An array representing the BQ row that was sent to prediction.
    predicted_values: An array resulting of calling hook_apply_formulas

  Returns:
    A JSON object, ideally containing all the information required for the
    activation phase.


      Example for offline conversions or conversion adjustment activation.

        row = {"clientId": original_data['clientId'],
         "orderId": original_data['orderId'],
         "gclid":original_data['gclid'],
         "date": str(original_data['time']),
         "conversionValue": predicted_values[0],
         "conversionValue2": predicted_values[1],
         "conversionValue3": predicted_values[2],
         "conversionValue4": predicted_values[3],
         "conversionValue5": predicted_values[4],
        }

        return row

  """
  del original_data, predicted_values  # Unused by default

  return {}


# Declare the auxiliary functions here i.e
#
# def apply_formula(pred_a, pred_b, real_value):
#  multiplier = FORMULA_PREDICTION_MULTIPLIER
#  if pred_a > pred_b:
#      return  max(real_value*(1-pred_a)/multiplier,multiplier*real_value)
#  else:
#      return  max((multiplier+pred_b)*real_value,multiplier*(1+real_value))
