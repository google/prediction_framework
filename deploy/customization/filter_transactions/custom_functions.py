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

"""This module builds the query to extract the relevant customer transactions.

It is used by filter_transactions cloud function to extract
the relevant customer transactions. Here it's possible to filter
new customers only, repeating customers only or all customer transactions.


 Add your imports here i.e

 import os
 from google.cloud import bigquery
 from datetime import datetime

 Declare the module global variables here i.e

 NEW_CUSTOMER_PERIOD = int(
     os.getenv('NEW_CUSTOMER_PERIOD', 120))
"""
from google.cloud import bigquery

from typing import List


def hook_get_load_data_query(table: str, current_date: str) -> str:
  """Returns the query to load the customer transactions.

  Here it is possible to filter just new customers or other customer
  groups.

  Args:
    table: A string representing the full path of the BQ table where the
      transactions are located. This table is the prepared periodic transactions
      table which contains a single line per customer.
    current_date: A string in YYYYMMDD format representing the date to process.
      This will be appended to the suffix of the table.
    
  Returns:
    A string with the query.

      Example:

      query = '''SELECT * FROM `{0}` s1 WHERE _TABLE_SUFFIX = "{1}"
        AND s1.gclid != "None"
        AND s1.clientId not in
        (select s2.clientId FROM `{0}` s2
        where _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%E4Y%m%d",
                     DATE_SUB(PARSE_DATE("%E4Y%m%d","{1}"),INTERVAL {2} DAY)
                     )
        AND FORMAT_DATE("%E4Y%m%d",
                        DATE_SUB(PARSE_DATE("%E4Y%m%d","{1}"),INTERVAL 1 DAY)
                        )
        )'''

        return query.format(table, current_date, new_customer_period)
  """
  del table, current_date  # Unused by default

  return ""


def hook_get_bq_schema() -> List[bigquery.SchemaField]:
  """Returns the schema of the periodic transactions table to be written in BQ.

  It's possible just to define those fields which type won't be autodetected.

  This function is only required if DATAFRAME_PROCESSING_ENABLED: 'Y'.

  Returns:
    An array of bigquery.SchemaField

      Example:

      return [
        bigquery.SchemaField('clientId', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('timeOnSite', 'FLOAT64', mode='NULLABLE'),
        bigquery.SchemaField('weekday', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('dayMoment', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('deviceCategory', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('browser', 'STRING', mode='REQUIRED')
      ]
  """
  return []
