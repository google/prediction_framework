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

"""This module is used by the predict_transaction cloud functions.

The aim is to calculate the final value of the prediction and define the schema
to write those predictions into BQ


 Add your imports here i.e

 import os
 from google.cloud import bigquery
 from datetime import datetime

 Declare the module global variables here i.e

 FORMULA_PREDICTION_MULTIPLIER = float(
     os.getenv('FORMULA_PREDICTION_MULTIPLIER', 0.0))
"""
import pandas as pd
from google.cloud import bigquery

def hook_get_load_predictions_query(table: str) -> str:
  """Returns the query that loads the predictions.

  The
  they are prepared for prediction. Ideally it's loading from the table
  BQ_LTV_ALL_PERIODIC_TX_TABLE with the suffix corresponding to a specific date.

  Args:
    table: A string representing the full path of the BQ table where the
      periodic transactions are located. This table is the
      BQ_LTV_ALL_PERIODIC_TX_TABLE with the suffix corresponding to a specific
      date. It usually has multiple lines per customer.

  Returns:
    A string with the query.

      Example:

        return f'''
          SELECT
                date,
                gclid,
                orderId,
                customerValue,
                predicted_customerBucketValue
            FROM
                `{table}`
            LIMIT
                10
            '''
  """

  return f"""SELECT * FROM `{table}` LIMIT 10"""


def hook_get_bq_schema() -> str:
  """Returns the schema of the table with the actionable predictions

  Returns:
    An array of bigquery.SchemaField

      Example:

        return [
            bigquery.SchemaField('orderId', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('gclid', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('date', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('customerValue', 'FLOAT64', mode='REQUIRED'),
            bigquery.SchemaField('conversionValue', 'FLOAT64', mode='REQUIRED')
        ]
  """
  return []


def hook_apply_formulas(predictions: pd.DataFrame) -> pd.DataFrame:
  """Applies as many formulas as required to the prediction values.

  Args:
    predictions: The data of the prediction table

  Returns:
    A dataframe containgin the results to be written to BQ final table.

      Example:

      If we use  classification and class A represents low value
      class B represents high value

      predictions['conversionValue'] = predictions.apply(
        lambda row: apply_formula(row), axis=1)
      predictions = predictions.drop(
      columns=['predicted_customerBucketValue']).reset_index(drop=True)

      return predictions
  """
  return predictions


def hook_on_completion(process_date: str):
  """Triggers any custom action after the result is written in to BQ
  Args:
    process_date: In format YYYYMMDD
  """


# Declare the auxiliary functions here i.e
#
# def apply_formula(predictions):
#
#  multiplier = FORMULA_PREDICTION_MULTIPLIER
#  pred_a = 0.0
#  pred_b = 0.0
#  real_value = predictions['customerValue']
#
#  if predictions['predicted_customerBucketValue'][0]['tables']['value'] == 'A':
#    pred_a = predictions['predicted_customerBucketValue'][0]['tables']['score']
#    pred_b = predictions['predicted_customerBucketValue'][1]['tables']['score']
#  else:
#    pred_a = predictions['predicted_customerBucketValue'][1]['tables']['score']
#    pred_b = predictions['predicted_customerBucketValue'][0]['tables']['score']
#
#  if pred_a > pred_b:
#    return  max(real_value*(1-pred_a)/multiplier,multiplier*real_value)
#  else:
#    return  max((multiplier+pred_b)*real_value,multiplier*(1+real_value))
