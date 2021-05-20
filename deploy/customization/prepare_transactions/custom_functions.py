"""This module is where the data is prepared for prediction."""

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

"""It is used by prepare_transactions cloud function to prepare each
transaction in order to be ready for prediction.
The code here must be pretty similar to the feature engineering code used for
the model training phase.

If custom csv data is uploaded into BQ this will be the place where to use it


 Add your imports here i.e

 import os
 from google.cloud import bigquery
 from datetime import datetime
 Declare the module global variables here i.e

 BQ_LTV_TRAINING_DATA_PRICE_AVGS_TABLE =
     os.getenv('BQ_LTV_TRAINING_DATA_PRICE_AVGS_TABLE', '')

 BQ_LTV_TRAINING_DATA_CAT_AND_BRANDS_TABLE = '{}.{}'.format(
    BQ_LTV_TABLE_PREFIX,
    str(os.getenv('BQ_LTV_TRAINING_DATA_CAT_AND_BRANDS_TABLE', ''))
    )
"""

import pandas as pd


def hook_get_bq_schema() -> str:
  """Returns the schema for the prepared_periodic_transactions table.

  Best practice is to write all the fields, which pretty much will match your
  model features, but if you have many features (+100) in your model just
  specifying those fields BQ have problems identifying the type, would suffice.

  Returns:
    An array of bigquery.SchemaField

      Example:

        return [
          bigquery.SchemaField('clientId','STRING',mode='REQUIRED'),
          bigquery.SchemaField('timeOnSite','FLOAT64',mode='NULLABLE'),
          bigquery.SchemaField('weekday','STRING',mode='REQUIRED'),
          bigquery.SchemaField('dayMoment','STRING',mode='REQUIRED'),
          bigquery.SchemaField('deviceCategory','STRING',mode='REQUIRED'),
          bigquery.SchemaField('browser','STRING',mode='REQUIRED')
          ]

  """
  return []


def hook_get_load_tx_query(table: str) -> str:
  """Returns the query that loads the transactions to aggregate.

  It loads all customer transactions and aggreagets it into a single row, so
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

        query = '''
          SELECT
            date as date
            ,time as time
            ,clientId as clientId
            ,cookieClientId as cookieClientId
            ,timeOnSite as timeOnSite
            ,deviceCategory as deviceCategory
            ,mobileDeviceBranding as mobileDeviceBranding
            ,operatingSystem as operatingSystem
            ,browser as browser
            ,parentTransactionId as parentTransactionId
            ,productSKU as productSKU
            ,productName as productName
            ,productCategory as productCategory
            ,category AS category
            ,productQuantity  as productQuantity
            ,productBrand as productBrand
            ,productPrice as productPrice
            ,productRevenue as productRevenue
            ,productCouponCode as productCouponCode
            ,productVariant as productVariant
            ,cm.index as cmIndex
            ,cm.value as cmValue
            ,city
            ,gclid
            ,channelGrouping
            ,campaignId
            FROM `{0}`
            '''
         return query.format(table)
  """
  del table  # Unused by default

  return ""


def hook_prepare(df: pd.DataFrame, model_date: str) -> pd.DataFrame:
  """Feature processing.

  Here the periodic transactions of a customer are aggregated into a single line.
  The code must be pretty similar to the one used for the feature engineering
  phase while training the model.

  Args:
    df: the pandas data frame representing the input data read from BQ
    model_date: the date of the model

  Returns:
    A pandas dataframe with 1 single row per customer

    Example of use:

      df['acquisitionDate'] = df['date']
      cat_and_brands_table = '{}_{}'.format(
            BQ_LTV_TRAINING_DATA_CAT_AND_BRANDS_TABLE,
            model_date)
      price_avg_table = '{}_{}'.format(
        BQ_LTV_TRAINING_DATA_PRICE_AVGS_TABLE,
        model_date
        )

      final_df = preprocess(df)
      final_df = merge_them_all(final_df,
                                cat_and_brands_table,
                                price_avg_table)
      return final_df

  """

  del model_date  # Unused by default

  return df


# Declare any auxiliary function below this line
# ----------------------------------------------
