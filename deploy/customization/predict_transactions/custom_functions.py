"""This module builds the query to load the batches of transactions to predict.

 Add your imports here i.e

 import os
 from google.cloud import bigquery
 from datetime import datetime
 Declare the module global variables here i.e

 FORMULA_PREDICTION_MULTIPLIER = float(
     os.getenv('FORMULA_PREDICTION_MULTIPLIER', 0.0))
"""


def hook_get_load_batch_query(table: str, start_index: int,
                              end_index: int) -> str:
  """Returns the query that loads the BQ data.

  Here it's possible to filter just new customers or other customer
  groups.

  Args:
    table: A string representing the full path of the BQ table where the
      transactions are located. This table is the prepared new customers periodic
      transactions table which contains a single line per customer.
    start_index: An integer representing the row number where to start
      retrieving data from
    end_index: An integer representing the row number where to stop retrieving
      data at

  Returns:
    A string with the query.

      Example:

        query = '''
          SELECT * FROM (

            SELECT s1.*, ROW_NUMBER() OVER() as rowId
            FROM `{0}` as s1
            LEFT JOIN (
              SELECT orderId, clientId, date
              FROM
                (
                 SELECT orderId, clientId, date,
                  ROW_NUMBER() OVER(PARTITION BY orderId, clientId, date ORDER
                  BY orderId asc) as rowId,
                 FROM `{0}`
                 ORDER BY orderId asc
                )
            ) s2 on s1.orderId=s2.orderId and s1.clientId=s2.orderId and
            s1.date=s2.date
          )
          WHERE rowId between {1} and {2}
          '''

        return query.format(table, start_index, end_index)
  """

  del table, start_index, end_index  # Unused by default

  return ""
