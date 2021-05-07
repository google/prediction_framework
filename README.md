# Prediction framework

## Summary 

LTV projects are likely to be very similar in structure between each other, so when building the solution for one of the top customers in Spain, we decided to create a “framework” which could be reused in other projects just by changing certain parts of such framework.

It’s not a perfect solution but it is intended to save large amounts of time.

Besides being initially designed for LTV prediction, it could be used for any kind of prediction by just removing some of the pieces.

**Author:** jaimemm

**Contributors: **pablogil

go/pltv-framework


## Prerequisites

The framework will fit almost all use cases under the following conditions:



*   Data source is stored in BQ \

*   Data output is stored in BQ
*   Feature engineering time for a single batch: &lt; 600 seconds (Cloud Function limit)
*   AutoML model is used (if BQML or Tensorflow small changes need to be done)
*   You are familiar with Python
*   The service account from the default Google project is used. That service account must be granted to access external resources. (If different SA needed, small changes will be required on the deployment process)


## What is this framework?

This framework is an effort to generalise all the steps involved in a prediction project, which requires daily processing, backfilling, throttling, synchronisation, storage and reporting so it could be reused, saving 80% of the development time.

The idea behind the framework is that with just a few particularizations/modifications (some of them important) the framework would fit any similar use case, with a high level or reliability.


## What does this framework do?




![alt_text](docs/resources/image2.png "image_tooltip")


Note: different projects are defined in the picture to represent our real first use case, but everything could be under the very same project for other use cases. The framework is flexible enough to address that scenario.



### UNDERSTANDING THE DIFFERENT STEPS



*   **<span style="text-decoration:underline;">Extract</span>**: this step will on a timely basis, query the transactions from the data source, corresponding to the run date (scheduler or backfill run date) and will store them in a new table into the local project BigQuery.
*   **<span style="text-decoration:underline;">Prepare:</span>** immediately after the daily transactions extract for one specific date is available, the data will be picked up from the local BigQuery and processed according to the specs of the model. Once the data is processed, it will be stored in a new table into the local project BigQuery.
*   **<span style="text-decoration:underline;">Filter</span>**: this step will query the data stored by the prepare process and will filter the required data and store it into the local project BigQuery. (i.e only taking into consideration new customers transactionsWhat a new customer is up to the instantiation of the framework for the specific use case. Will be covered later).
*   **<span style="text-decoration:underline;">Predict</span>**: once the new customers are stored, this step will read them from BigQuery and call the prediction using the AutoML API. A formula based on the result of the prediction could be applied to tune the value or to apply thresholds. Once the data is ready, it will be stored into the BigQuery within the target project.
*   **<span style="text-decoration:underline;">Post\_process (only for AutoML Batch mode):</span>** A formula could be applied to the AutoML batch results to tune the value or to apply thresholds. Once the data is ready, it will be stored into the BigQuery within the target project.
*   **<span style="text-decoration:underline;">Stop model (only for AutoML Live mode):</span>** AutoML models need to be deployed to make predictions and every running minute has a cost associated. Limiting that cost is the purpose of this step, which will query periodically if the model is deployed and no predictions on the fly, so the model could be stopped.



### UNDERSTANDING THE BQ TABLES

All tables are sharded tables by date:

**<span style="text-decoration:underline;">all\_daily\_transactions\_YYYYMMDD</span>**: stores all the transactions corresponding to the execution date. Since data is lagged the date of the data with the table will not correspond with the date of the table name. The schema will depend on the particular use case query.

**<span style="text-decoration:underline;">prepared\_daily\_transactions\_YYYYMMDD</span>**: it will store the transactions for one single day, the transactions from all\_daily\_transactions\_YYYYMMDD are aggregated and should include those metrics required by your model to perform the prediction. The schema will depend on the particular use case. The idea is to get the same table structure and data that you will pass when calling AutoML.

 \


**<span style="text-decoration:underline;">prepared\_new\_customers\_daily\_transactions\_YYYYMMDD</span>**: it’s intended to store those transactions from new customers only. If your use case requires to predict only on new customers, include them into this table filtering old onee using the corresponding SQL. Otherwise just select \* from prepared\_daily\_transactions\_YYYYMMDD. The schema will depend on the particular use case query.

**<span style="text-decoration:underline;">predictions\_YYYYMMDD</span>**: this table contains the LTV predictions for the transactions in prepared\_new\_customers\_daily\_transactions\_YYYYMMDD. Since the ultimate purpose of this table is to feed GAds, Campaign Manager, SA360 or any other system interested into the conversion value, the schema will look similar to the following one (it could be customised):


<table>
  <tr>
   <td>Field name
   </td>
   <td>Type
   </td>
   <td>Mode
   </td>
  </tr>
  <tr>
   <td>clientId
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>orderId
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>gclid
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>date
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>conversionValue
   </td>
   <td>FLOAT
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>conversionValue2
   </td>
   <td>FLOAT
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>conversionValue3
   </td>
   <td>FLOAT
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>conversionValue4
   </td>
   <td>FLOAT
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>conversionValue5
   </td>
   <td>FLOAT
   </td>
   <td>REQUIRED
   </td>
  </tr>
</table>


**<span style="text-decoration:underline;">metadata: </span>** stores the information regarding the model to use, which will be used to correlate with the features. The schema is fixed:


<table>
  <tr>
   <td>Field name
   </td>
   <td>Type
   </td>
   <td>Mode
   </td>
  </tr>
  <tr>
   <td>export_date
   </td>
   <td>STRING
   </td>
   <td>NULLABLE
   </td>
  </tr>
  <tr>
   <td>model_date
   </td>
   <td>STRING
   </td>
   <td>NULLABLE
   </td>
  </tr>
  <tr>
   <td>model_name
   </td>
   <td>STRING
   </td>
   <td>NULLABLE
   </td>
  </tr>
</table>


Where 



*   export\_date: indicates the latest full data export for training happened. It will be used for automatic model training in the future. \

*   model\_date: indicates the data used to train the model. This correlates with the features table. \

*   model\_name: the name of the model in AutoML 

Example:


<table>
  <tr>
   <td>Row
   </td>
   <td>export_date
   </td>
   <td>model_date
   </td>
   <td>model_name
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td>1
   </td>
   <td>20200605
   </td>
   <td><em>null</em>
   </td>
   <td><em>null</em>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td>6
   </td>
   <td><em>null</em>
   </td>
   <td>20200605
   </td>
   <td>training_data_20200605_0608
   </td>
   <td>
   </td>
  </tr>
</table>


**<span style="text-decoration:underline;">training\_data\_features\_YYYYMMDD:</span>** this table stores all the features used by the model. They are needed for the prediction phase. The suffix YYYYMMDD must match the model data set in the metadata. The suggested schema is as follows, but it could be customised:


<table>
  <tr>
   <td>Field name
   </td>
   <td>Type
   </td>
   <td>Mode
   </td>
  </tr>
  <tr>
   <td>value
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
  <tr>
   <td>type
   </td>
   <td>STRING
   </td>
   <td>REQUIRED
   </td>
  </tr>
</table>


Example: 


<table>
  <tr>
   <td>Row
   </td>
   <td>value
   </td>
   <td>type
   </td>
  </tr>
  <tr>
   <td>1
   </td>
   <td>cat20093
   </td>
   <td>category
   </td>
  </tr>
  <tr>
   <td>2
   </td>
   <td>nestle
   </td>
   <td>brand
   </td>
  </tr>
</table>


Why this table choice? 



1. Because it is very easy to report on: \

*   The increase on the number of conversions and new customer conversions
*   The increase of AOV in overall and new customer conversions
*   The relationship between different types of conversions
2. Any kind of historical query is easier and cheaper than performing them over massive data sources (i.e 7TB of Google analytics)

If extra tables are needed it is possible to create and use them from the customization code.


### 


### UNDERSTANDING THE FIRESTORE COLLECTIONS

Firestore is used for throttling, wait & notify and synchronize patterns.

There are 4 different collections used:

**<span style="text-decoration:underline;">prepare\_daily\_tx\_tracking</span>**: it logs the activities which are still in prepare data. It is used to not trigger the extraction of new customer transactions until all “prepare” activities are completed. To decide if a customer is new or not, usually a time window is considered, therefore the past data must be stored before looking to query such time window.

**<span style="text-decoration:underline;">prediction\_tracking</span>**: this collection is used to track if any prediction task is still running, so the stop model process does not stop the AutoML model while prediction is happening.

**<span style="text-decoration:underline;">long\_running\_tasks</span>**: this collection is used to store the messages which have been throttled or pending on a long running operation like AutoML model deploy.

**<span style="text-decoration:underline;">unitary\_prediction\_tracking\_&lt;prediction\_table>: </span>** this is just for traceability purposes, in case something would fail. Only for AutoML Live scenario. \


Collection names examples:

food\_pltv\_prepare\_daily\_tx\_tracking

food\_pltv\_long\_running\_tasks

food\_pltv\_prediction\_tracking

food\_pltv\_unitary\_prediction\_tracking\_test-ltv-deploy.ltv\_ml.predictions\_20200511


## What can I do with the predictions?

As mentioned before, the ultimate purpose of this setup is to feed GAds, Campaign Manager, SA360 or any other system interested in conversion values, with the system predicted conversion values.

All predicted data will be available in the BQ predictions\_YYYYMMDD tables, so depending on the case there will be various ways to feed the data to the target system (APIs, file transfers….), but when using Google products one of the easiest and most flexible ways of uploading conversions will be by using the open source tool: [Tentacles](https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/activation/gmp-googleads-connector).


## How to instantiate the framework for your use case


### Directory structure

There’re 2 main directories: “cfs” containing the cloud functions code (not to be altered unless extending the framework) and “deploy” directory, containing all the files required for the deployment.


![alt_text](docs/resources/image7.png "image_tooltip")



### Training phase

_Note: Automatic training and metadata update is in the roadmap_


### Prediction phase

At this point you must have: \




*   An AutoML model.
*   The set of features used by the model (AutoML dataset?).
*   Any extra calculated data required by the model during training (price averages etc…)


#### Environment Variables Setting

Set the deployment name and solution prefix. This will be used to name all the GCP resources to be created:


        # General settings


        DEPLOYMENT\_NAME: 'food'


        SOLUTION\_PREFIX: 'pltv’

Set the service account to be used in all CFs. If it does not exist yet, do not include the domain, but just the name you want for the service account. (If different accounts needed for different projects, changed must be made in the code) \



        SERVICE\_ACCOUNT: '1234567-compute@developer.gserviceaccount.com'

Set the GCP project and region where all the main processing will take place (cloud functions, task poller, writer and firestore).


    DEFAULT\_GCP\_PROJECT: ‘my-google-project’


    DEFAULT\_GCP\_REGION: 'europe-west1'

Set data source GCP project, dataset and tables: \


BQ\_DATA\_SOURCE\_GCP\_PROJECT: the project where the data source is.

BQ\_DATA\_SOURCE\_DATA\_SET: the dataset where the data source is.

BQ\_DATA\_SOURCE\_TABLES: the tables template to use as a data source.

 \
	Sample values:


        # BQ Data source settings


        BQ\_DATA\_SOURCE\_GCP\_PROJECT: ‘myclient-123456'


        BQ\_DATA\_SOURCE\_DATA\_SET: '1234567'


        BQ\_DATA\_SOURCE\_TABLES: 'ga\_sessions'

Set the GCP dataset and region (used for data transfer) where all the intermediate processing is going to happen (it could be different from data source project):


    BQ\_LTV\_GCP\_PROJECT: the project used to store the data and the scheduled query.


    BQ\_LTV\_GCP\_BROAD\_REGION: region used to create the datasets.


    BQ\_LTV\_DATASET: dataset name to be used for all the generated tables.


    Sample Values:


        # BQ LTV settings


    BQ\_LTV\_GCP\_PROJECT: ‘customer-project’


    BQ\_LTV\_GCP\_BROAD\_REGION: 'EU'


        BQ\_LTV\_DATASET: 'pltv'

Set model configuration: \


MODEL\_GCP\_PROJECT: the project where the model resides.

MODEL\_REGION: the region of the project where the model resides.

MODEL\_AUTOML\_API\_ENDPOINT: the endpoint according to the region.


    MODEL\_NEW\_CLIENT\_DAYS: number of days which will define the cohort for new customers.


    Sample values:


        # Model Settings


        MODEL\_GCP\_PROJECT: 'my-model-project


        MODEL\_REGION: 'eu'


        MODEL\_AUTOML\_API\_ENDPOINT: 'eu-automl.googleapis.com:443'


        MODEL\_NEW\_CLIENT\_DAYS: '365'

Set scheduler settings: \



    TIMEZONE: the timezone used to create the schedulers.


    MODEL\_STOPPER\_POLLER\_CONFIG: cron syntax for model stopper scheduler.


    DATA\_SOURCE\_DAILY\_TX\_POLLER\_CONFIG: cron syntax for model data source extractor scheduler.


    LONG\_RUNNING\_TASKS\_POLLER\_CONFIG: cron syntax for polling waiting tasks.


    DISCARD\_TASKS\_OLDER\_THAN\_HOURS: expiration time in hours for the waiting tasks


    Sample values: \



        # Polling & throttling settings


        MODEL\_STOPPER\_POLLER\_CONFIG: '0 \\*/1 \\* \\* \\*'


        DATA\_SOURCE\_DAILY\_TX\_POLLER\_CONFIG: '0 \\*/1 \\* \\* \\*'


        LONG\_RUNNING\_TASKS\_POLLER\_CONFIG: '\\*/2 \\* \\* \\* \\*'


        DISCARD\_TASKS\_OLDER\_THAN\_HOURS: '23'


        TIMEZONE: 'Europe/Madrid'

Set the amount of minutes the stopper has to consider as inactivity, after the latest prediction task activity:


    STOP\_MODEL\_WAITING\_MINUTES: ‘30’


##### Tuning parameters

In order to avoid quota problems (max. concurrent requests, max. connections open …) throttling needed to be in place.

###### Polling & throttling settings

**DATA\_SOURCE\_DAILY\_TX\_POLLER\_CONFIG**: '0 \*/1 \* \* \*' → the daily poller that triggers the daily export. Default every hour. \


**LONG\_RUNNING\_TASKS\_POLLER\_CONFIG**: '\*/2 \* \* \* \*' → the poller to check the enqueued tasks (either intentionally delayed or long running ones). Default 2 minutes. \


**DISCARD\_TASKS\_OLDER\_THAN\_HOURS**: '24' → how long a task will be waiting in the “queue” (if running daily reports, more than 24 hours does not make sense) \


**MAX\_TASKS\_PER\_POLL**: '10' → the number of “enqueued” tasks to retrieve on every poll. Default: 10. Increase it wisely.

**DELAY\_PREPARE\_DAILY\_IN\_SECONDS**: '60' → minimum delay to be introduced on the prepare daily task.  The execution time will depend on max(DELAY\_PREPARE\_DAILY\_IN\_SECONDS, LONG\_RUNNING\_TASKS\_POLLER\_CONFIG) and the position of the task into the “queue”, affected by MAX\_TASKS\_PER\_POLL \


**DELAY\_EXTRACT\_NEW\_CUSTOMERS\_DAILY\_IN\_SECONDS**: '60'→ minimum delay to be introduced on the extract new customers transactions daily task.  The execution time will depend on max(DELAY\_PREPARE\_DAILY\_IN\_SECONDS, LONG\_RUNNING\_TASKS\_POLLER\_CONFIG) and the position of the task into the “queue”, affected by MAX\_TASKS\_PER\_POLL \
 \


**DELAY\_PREDICT\_TRANSACTIONS\_IN\_SECONDS**: '60'→ minimum delay to be introduced on the predict transactions task.  The execution time will depend on max(DELAY\_PREPARE\_DAILY\_IN\_SECONDS, LONG\_RUNNING\_TASKS\_POLLER\_CONFIG) and the position of the task into the “queue”, affected by MAX\_TASKS\_PER\_POLL \


**MAX\_PREDICTION\_BATCH\_SIZE**: '500' → the number of rows to be queried per batch. Each batch is sent on a record by record basis to AutoML prediction.


##### Custom variables

This section is specific for those variables to be used in the [custom code](#bookmark=id.e12jiptc2973) and 

[extra data](#extra-data-settings) sections. I.e: \


BQ\_LTV\_TRAINING\_DATA\_PRICE\_AVGS\_TABLE: 'training\_data\_price\_avgs'


#### Code Customisation



*   Replace data transfer SQL query which extracts the daily transactions in:

    _deploy/customization/queries/extract\_all\_daily\_transactions.sh_.

*   If required replace/modify the function: \

    *   **_hook\_get\_load\_data\_query_**

In _deploy/customization/extract\_new\_customers\_daily\_transactions/[custom\_functions.py](https://cs.corp.google.com/piper///depot/google3/experimental/users/jaimemm/carrefour_groceries_ltv/deploy/customization/extract_new_customers_daily_transactions/custom_functions.py?ws=jaimemm/34)_



*   Modify/Replace the functions:
    *   **_hook\_get\_load\_tx\_data\_query_** and 
    *   **_hook\_prepare_**

    In _deploy/customization/prepare\_daily\_transactions/[custom\_functions.py](https://cs.corp.google.com/piper///depot/google3/experimental/users/jaimemm/carrefour_groceries_ltv/deploy/customization/extract_new_customers_daily_transactions/custom_functions.py?ws=jaimemm/34)_


     \
This is the key part, a strong dependency is set between this step and the features and extra data loaded into BQ.

*   If required, replace/modify the function: \

    *   **_hook\_get\_load\_batch\_query_**

In _deploy/customization/predict\_transactions/[custom\_functions.py](https://cs.corp.google.com/piper///depot/google3/experimental/users/jaimemm/carrefour_groceries_ltv/deploy/customization/extract_new_customers_daily_transactions/custom_functions.py?ws=jaimemm/34)_



*   Modify/Replace the functions:
    *   **_hook\_apply\_formulas_** and 
    *   **_hook\_get\_bq\_schema_**

    In_ deploy/customization/predict\_transaction/[custom\_functions.py](https://cs.corp.google.com/piper///depot/google3/experimental/users/jaimemm/carrefour_groceries_ltv/deploy/customization/extract_new_customers_daily_transactions/custom_functions.py?ws=jaimemm/34)_





#### Features Settings

Inside _deployment/customization/data _directory, fill 


    **_training\_data\_features.csv_** 

With the features list used to train your model, without any header row. The structure is as follows: \




        *   First column: feature name
        *   Second column: feature type

		I.e 

			cat200043, category

			fruits, category

			nestle, brand

			bmw, brand


#### Extra Data Settings



*   Create a .csv file inside _deployment/customization/data _directory, with the data you will require. I.e: _training\_data\_price\_averages.csv_

            cat170025,g,1.9509090909090909


            cat180024,g,10


            cat190009,g,3.8174402730375325

*   Create a “.sh” file inside _deployment/customization/scripts (i.e populate\_price\_averages.sh),_ which loads the data into the BQ table. \

    *   Use the variables defined in [custom variables](#bookmark=id.8ta3hbclyl55) \

    *   Make sure the code in [custom code](#bookmark=id.e12jiptc2973), is able to read the data properly \

    *   Example:

        ```
        #it's executed from deploy path
        source ./env.sh
        
        
        bq --project_id=$BQ_LTV_GCP_PROJECT load  --replace=true $BQ_LTV_DATASET"."$BQ_LTV_TRAINING_DATA_PRICE_AVGS_TABLE"_"$MODEL_DATE \
         ./customization/data/training_data_price_avgs.csv \
         category:STRING,variantUnits:STRING,productPriceQuantityRatio:FLOAT
        
        if [ $? -ne 0 ]; then
          echo "Error populating features BQ table. Exiting!"
          exit -1
        fi
        ```


*   Add a call to the new .sh file in the custom\_deploy.sh script, as follows:

    ```
    source ./env.sh
    
    #custom scripts calls
    customization/scripts/populate_price_averages.sh
    
    ```




#### Post Model Stop Actions

If any task needs to be done after the model is stopped (i.e copy one table to a different project), just modify/replace the function:

**_hook\_post\_stop\_action_**

 In_ deploy/customization/stop\_model/[custom\_functions.py](https://cs.corp.google.com/piper///depot/google3/experimental/users/jaimemm/carrefour_groceries_ltv/deploy/customization/extract_new_customers_daily_transactions/custom_functions.py?ws=jaimemm/34)_


### DEPLOYMENT

In Firestore screen, make sure you have enabled Firestore in Native Mode


![alt_text](docs/resources/image15.png "image_tooltip")


Once the configuration is set, get into “deploy” directory (cd deploy)  and execute:

 **_sh deploy.sh _ **

You will need to have GCP CLI tools installed.

A successful deployment will output the following results in the console:

Now deploying \_pLTV\_scaffolding\_ ...

<>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>&lt;>

Updated property [core/project].

Updated property [compute/region].

Operation "operations/acf.98066e2d-4543-42dd-ac33-8d3e0d5cf7d0" finished successfully.

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* Services Enabled. Waiting for changes to be applied...     \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* Service Account Created.                                                          \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

Updated IAM policy for project [test-ltv-deploy].

Updated IAM policy for project [test-ltv-deploy].

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* Account Permissions Set.                                                         \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* Cloud Functions Successfully Deployed.                                \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* Schedulers Successfully Deployed.                                         \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* BQ Dataset Successfully Deployed. Waiting to be available.\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\* BQ Elements Successfully Deployed.                         \*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

 IMPORTANT: run the post deployment tasks explained in the doc! 

 IMPORTANT: xxxx@yyy.iam.gserviceaccount.com on external resources!!

\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*



The following resources must be created into the GCP Project: \




*   IAM account

 

![alt_text](docs/resources/image13.png "image_tooltip")


*   Data Transfer

    


![alt_text](docs/resources/image3.png "image_tooltip")


*   Topics

    

![alt_text](docs/resources/topics.png "image_tooltip")


*   Cloud Functions

    


![alt_text](docs/resources/cloud_functions.png "image_tooltip")


*   Schedulers \
 \


![alt_text](docs/resources/schedulers.png "image_tooltip")



### 


### POST-DEPLOYMENT TASKS

Assign pub sub notification to_ e[xtract\_all\_daily\_transactions](https://pantheon.corp.google.com/bigquery/scheduled-queries/locations/europe/configs/5f75b24d-0000-2414-b9af-94eb2c0801e8/runs?project=carr-ltv-jaimemm)_ configuration



*   Go to Pub/Sub > Topics
*   Copy the name of the topic that ends with _daily\_extract\_ready_




![alt_text](docs/resources/copy_topic.png "image_tooltip")




*   Go to BigQuery > Scheduled queries. 
*   Click on _extract\_all\_daily\_transactions _scheduled query.
*   Click on the “Edit” button on the upper-right corner.




![alt_text](docs/resources/edit_query.png "image_tooltip")




*   In the new displayed page, click on “update scheduled query”




![alt_text](docs/resources/update_query.png "image_tooltip")




*   Paste the name of the pub/sub notification topic previously copied and click the update button




![alt_text](docs/resources/paste_topic.png "image_tooltip")




## Running your customized framework


### Processing requests

There’re two main ways of triggering a request:



*   Trigger the daily scheduler (time triggered or manual)
*   Trigger backfilling (see backfilling section)


### Backfilling

Just use BigQuery scheduled queries backfilling on the data transfer _extract\_all\_daily\_queries._



*   Go to BigQuery scheduled queries and click on the _extract\_all\_transactions_

    


![alt_text](docs/resources/image5.png "image_tooltip")


*   Click on “More >  Schedule backfill”




![alt_text](docs/resources/image6.png "image_tooltip")




*   Select the timeframe (remember: start date included, end date excluded) and click OK.




![alt_text](docs/resources/image10.png "image_tooltip")




*   Soon messages will appear on the topic &lt;DEPLOYMENT\_NAME>.&lt;SOLUTION\_PREFIX>.daily\_extract\_ready

Note: not tested on batches of more than 1 month


### How does a successful run look like?

Let’s say we either backfill the process for the date 20200710 or is triggered by the scheduler, the following will happen:



1. all\_daily\_transactions\_20200710 table is created in the BQ\_LTV\_GCP\_PROJECT project under BQ\_LTV\_DATASET \

2. After the minutes in LONG\_RUNNING\_TASKS\_POLLER\_CONFIG, prepared\_daily\_transactions\_20200710 table is created in the BQ\_LTV\_GCP\_PROJECT project under BQ\_LTV\_DATASET
3. Again after LONG\_RUNNING\_TASKS\_POLLER\_CONFIG minutes, prepared\_new\_customers\_daily\_transactions\_20200710 is created in the BQ\_LTV\_GCP\_PROJECT project under BQ\_LTV\_DATASET
4. A request for model deployment will happen.
5. After a few minutes from the model starts,  predictions\_20200710 is created and started to be populated in BQ\_LTV\_GCP\_PROJECT under BQ\_LTV\_DATASET.
6. Around STOP\_MODEL\_WAITING\_MINUTES after the last prediction request action, the model is stopped.

Let’s say we run a backfill process of 30 days:



*   Steps 1 and 2 stay the same.
*   3 will not be triggered until the 30 tasks for step 2 are completed (do not get stressed if the process does not seem to progress )
*   4, 5 and 6 stay the same.


### Tracing requests

**<span style="text-decoration:underline;">Logs</span>**

Every cloud function leaves a trace based on the date belonging to the data which is being processed. So the best way to check the status of a particular date data processing is just to filter based on the date string.


**<span style="text-decoration:underline;">Firestore</span>**

Firestore tables could be helpful to trace request stages (particularly for predictions completion).



## FAQ

### “I execute a backfill but I do not see the tables populated”**



1. Check there’re no old tasks logged into firestore **<span style="text-decoration:underline;">prepare\_daily\_tx\_tracking</span>**
    1. If there are tasks which do not belong to your current run
        1. If the dates are in the future of your current run, delete them and start the backfilling from that data
        2. If the dates are in the past of your current run
            *   Clean all the firestore tables (you can use the scripts in the util directory)
            *   Run the backfill from the oldest date that was in firestore. The process is idempotent so no need to delete tables. In case, for clarity purposes, you’d like to delete the tables for a date range there’re some scripts under “utils” directory which will help.
    2. If there’re no tasks:
        3. Check the logs for one of the dates and identify the process it stops at
        4. If BQ tables are not created, check the projects configuration in the config.yaml file
        5. Check the topics for incoming traffic
        6. Check if the model has been started correctly

### “There are tables with no schema on extract\_daily\_transactions\_YYYYMMDD”**

 \
When executing the data transfer in BQ it will create/overwrite the target table with the resulting data from the query, even if empty. As per today, there’s no way to avoid blank table creation. This behaviour is not affecting the further steps, since checks have been included and  there’s no query on extract\_daily\_transactions\_\* 

### “There are less tables in prepared\_daily\_transactions\_YYYYMMDD than in extract\_daily\_transactions\_YYYYMMDD”**

That’s because of the empty tables in extact\_daily\_transactions:YYYYMMDD, which are ignored.

### “There are less tables in prepared\_new\_customers\_daily\_transactions\_YYYYMMDD than in prepared\_daily\_transactions\_YYYYMMDD”**

That’s caused by no new customers on some dates.

### “Why not using AutoML batch prediction”**

Batch prediction has a very low concurrent API quota, which made backfilling exceed that quota.

Also, in the scenario where predictions have to be written on clients BQ projects, usually the access is granted to one dataset. AutoML batch prediction creates one dataset per request, which overcomplicates the whole solution.

### “I want to use AutoML batch prediction anyway”**

You will need to modify or create a separate _predict\_transaction/main.py _(predict\_transaction without “s”)  which will call AutoML batch prediction (there's a sample file included _predict\_transactions\_ltv\_batch.py_).  \
With batch prediction you might want to connect the “extract new customers transactions” step directly to “predict transaction” (skipping predict\_transaction**s,** see [diagrams](#bookmark=id.gmwwbsvvhnzw)). This can be achieved just by changing the topic values in the config file. Also, make sure to:



*   Throttle requests when backfilling (only 5 requests per minute). You can use the enqueue operation  used in other steps. \

*   Pass the necessary information to reconstruct the AutoML output dataset and table name.
*   Disable the food\_pltv\_model\_stopper Cloud Scheduler

### “I want to use BQML prediction”**

See [“I want to use AutoML batch prediction anyway”](#bookmark=id.6x3fub8xnch9)

### “I want to use my Tensorflow model for prediction”**

This has not been tested yet, but the steps would be similar to the ones in the following list:



*   Change _start\_model_ and _get\_model\_status _function in _cfs/predict\_transactions/main.py._
*   Change _stop\_model_ function in _cfs/stop\_model/main.py. \
_
*   Modify or create a separate _predict\_transaction/main.py _to call the Tensorflow deployed model API.

### “Why post-deployment tasks are needed”**

 \
CLI BQ scheduled query creation does not allow to pass pub/sub notification topics.

### “I receive the following error: The Cloud Firestore API is not available for Datastore Mode projects.”**

Have you enabled the Native mode of Firestore.

