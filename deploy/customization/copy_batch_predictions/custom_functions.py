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

def hook_get_load_predictions_query(table: str) -> str:
  """Returns the query that loads the predictions.

  The 
  they are prepared for prediction. Ideally it's loading from the table
  BQ_LTV_ALL_PERIODIC_TX_TABLE with the suffix corresponding to a specific date.

  Args:
    table: A string representing the full path of the BQ table where the periodic
      transactions are located. This table is the BQ_LTV_ALL_PERIODIC_TX_TABLE with
      the suffix corresponding to a specific date. It usually has multiple lines
      per customer.

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
  

  return f'''select * from `{table}` limit 10'''


def hook_get_bq_schema() -> str:
  """Returns the schema of the table with the actionable predictions 
  
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


def hook_apply_formulas(predictions: pd.DataFrame) -> pd.DataFrame:
  """Applies as many formulas as required to the prediction values.

  Args:
    predictions: The data of the prediction table

  Returns:
    A dataframe containgin the results to be written to BQ final table.

      Example:

      If we use  classification and class A represents low value
      class B represents high value

      real_value = predictions["customerValue"]
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

  return predictions





# Declare the auxiliary functions here i.e
#
# def apply_formula(pred_a, pred_b, real_value):
#  multiplier = FORMULA_PREDICTION_MULTIPLIER
#  if pred_a > pred_b:
#      return  max(real_value*(1-pred_a)/multiplier,multiplier*real_value)
#  else:
#      return  max((multiplier+pred_b)*real_value,multiplier*(1+real_value))
