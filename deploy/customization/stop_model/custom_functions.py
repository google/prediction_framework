"""This module executes post model stopped acitvities.

It is used by stop_model cloud function to execute any necessary
post model stopped activity: house keeping, write data into another location...
"""

from typing import Any, Dict, Optional
import google.cloud.functions.Context

# Add your imports here i.e
#
# import os
# from google.cloud import bigquery
# from datetime import datetime

# Declare the module global variables here i.e
#
# PREDICTIONS_TABLE_GCP_PROJECT = str(
#    os.getenv('PREDICTIONS_TABLE_GCP_PROJECT',
#              '')
#    )


def hook_post_stop_action(event: Dict[str, Any],
                          context=Optional[google.cloud.functions.Context]):
  """Executes after the model is stopped.

  The only information available at this execution moment is the pub/sub event
  from the scheduler which triggered the stop_model cloud function.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  Example of use:
    Writing data into another project:  publish_date = get_date(context)
    print('Received: {}'.format(publish_date))  predictions_table =
      '{}_{}'.format( BQ_LTV_PREDICTIONS_TABLE, publish_date )
      target_table_full_path = '{}.{}.{}'.format( PREDICTIONS_TABLE_GCP_PROJECT,
      PREDICTIONS_TABLE_DATASET, predictions_table )
      write_on_external_project(BQ_LTV_GCP_PROJECT, BQ_LTV_DATASET,
      predictions_table, target_table_full_path)
  """

  del event, context  # Unused by default
