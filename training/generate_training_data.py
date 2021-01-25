import os
import datetime
import time
import calendar
import pandas as pd
import numpy as np
import re
import pytz
import math
import datetime


from google.cloud import bigquery

BUCKET = 'carr-ltv-jaimemm_ml'
PROJECT = 'carr-ltv-jaimemm'
REGION = 'europe-west1'
NUM_CATEGORIES=200
NUM_BRANDS=200
NUM_DAYS=90
REVENUE_THRESHOLD=300


DATASET_MOD_SPLIT=10
DATASET_MOD_TRAINING_SPLIT=0
DATASET_MOD_EVAL_SPLIT=0

dt = datetime.datetime.today()
#CURRENT_DATE = "20200601"#dt.strftime("%Y%m%d")

START_DATE=20190401
END_DATE=20200310

# [0..8] --> training 90%
# [9] --> 10%


AMOUNT_OF_DATA=1 #number between 0 and 1 where 1=100% and 0=0%

BQ_TABLE_PREFIX="carr-ltv-jaimemm.ltv_ml."
INOUT_BQ_METADATA_TABLE=BQ_TABLE_PREFIX+"metadata"


CURRENT_DATE=""

INPUT_BQ_TRANSACTIONS_TABLE=BQ_TABLE_PREFIX+"transactions_"
OUTPUT_BQ_CLIENTS_TABLE=BQ_TABLE_PREFIX+"processed_clients"
OUTPUT_BQ_TABLE_PREFIX=BQ_TABLE_PREFIX+"training_data"
OUTPUT_BQ_DATA_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_"
OUTPUT_BQ_FEATURES_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_features_"
OUTPUT_BQ_PRICE_AVGS_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_price_avgs_"



#----------------------------------------------------------------------------------------------------------------
# This function gets the categories and brands used for training the model
#----------------------------------------------------------------------------------------------------------------

def load_metadata():
    query = """
    SELECT
       FORMAT_DATE('%E4Y%m%d',max(PARSE_DATE('%E4Y%m%d', export_date))) export_date
      FROM
        {0} LIMIT 1"""
    query=query.format(INOUT_BQ_METADATA_TABLE)

    df = bigquery.Client().query(query).to_dataframe()
    #df = df.reset_index(drop=True)
    return df



#----------------------------------------------------------------------------------------------------------------
# This function checks if the string is a number
#----------------------------------------------------------------------------------------------------------------
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#----------------------------------------------------------------------------------------------------------------
# This function transalates a timestamp to night,morning etc....
#----------------------------------------------------------------------------------------------------------------
def get_day_moment(timestamp):
    h=int(time.strftime("%H", time.localtime(timestamp)))

    if (h>=0 and h<6):
        res='Night'
    elif (h>=6 and h<13):
        res='Morning'
    elif (h>=13 and h<17):
        res='Afternoon'
    elif (h>=17 and h<21):
        res='Evening'
    elif (h>=21 and h<24):
          res='Night'

    return res

#----------------------------------------------------------------------------------------------------------------
# This function calculates the name of the week day based on the timestamp
#----------------------------------------------------------------------------------------------------------------
def find_day(date):
    wkd = datetime.datetime.strptime(date, '%Y%m%d').weekday()
    return (calendar.day_name[wkd])


#----------------------------------------------------------------------------------------------------------------
# This function gets the top categories based on number of items bought in the dataset
#----------------------------------------------------------------------------------------------------------------
def get_top_cats_per_quantity(df,limit):
    res=df.groupby(['category'])['productQuantity'].sum().reset_index().sort_values('productQuantity',ascending=False).head(limit)
    res=res.drop(columns={'productQuantity'}).reset_index()
    return res


#----------------------------------------------------------------------------------------------------------------
# This function gets the top brands based on all the purchases in the dataset
#----------------------------------------------------------------------------------------------------------------
def get_top_brands(df,limit):
    res=df.groupby(['productBrand'])['productRevenue'].sum().reset_index().sort_values('productRevenue',ascending=False).head(limit)
    res=res.drop(columns={'productRevenue'}).reset_index()
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the top categories based on the revenue of all the purchases in the dataset (currently not used)
#----------------------------------------------------------------------------------------------------------------
def get_top_cats_per_revenue(df,limit):
    res=df.groupby(['category'])['productRevenue'].sum().reset_index().sort_values('productRevenue',ascending=False).head(limit)
    res=res.drop(columns={'productRevenue'}).reset_index()
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the list of browser
#----------------------------------------------------------------------------------------------------------------
def get_browsers(df):
    res=df['browser'].unique()
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the list of operatingSystem
#----------------------------------------------------------------------------------------------------------------
def get_operating_systems(df):
    res=df['operatingSystem'].unique()
    res.apply(lambda x: "MiCarrefour" if ("carrefour" in x) else x)
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the list of mobileDeviceBranding
#----------------------------------------------------------------------------------------------------------------
def get_mobile_brand(df,limit):
    res=df['mobileDeviceBranding'].unique()
    res.fillna(values="Other")
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the list of deviceType
#----------------------------------------------------------------------------------------------------------------
def get_device_type(df,limit):
    res=df['deviceCategory'].unique()
    return res



def preprocess(ddf):

  df=ddf.copy()
  df['weekday']=df['date'].apply(lambda x: find_day(x))
  df['dayMoment']=df['time'].apply(lambda x: get_day_moment(x))

#  print("DONE1!")
#  print(df['weekday'])

  list = ['gm','gr','g','kg','k','ml','cl','l']


  df['variantQuantity']= df['productVariant'].str.extract('([0-9]+x[0-9]+|[0-9]+)')
  df[['variantQ1','variantQ2']]=df['variantQuantity'].str.split("x",expand=True)

  df['variantQ1']=np.where(df['variantQ1'].isna(),1,df['variantQ1'].astype(float))
  df['variantQ2']=np.where(df['variantQ2'].isna(),1,df['variantQ2'].astype(float))

  df['variantQuantity']=df['variantQ1'] * df['variantQ2']

  df['variantUnits']= df['productVariant'].str.extract(r'(\d+(?:\.\d{1,2})(-)?)*(toallitas|comprimidos|rollo|recambios|capsulas|cacitos|pastillas|unidades|botellas|lavados|piezas|pzas|cl|ml|ud|g[m|r]?|kg|cm|l)')[2]

  df['variantUnits'] = np.where(df['variantUnits'].isin(list)
                               ,df['variantUnits']
                               ,'units')


  conditions = [
    df['variantUnits'].str.contains("ml",regex=False) | df['variantUnits'].str.contains("gr",regex=False) | df['variantUnits'].str.contains("g",regex=False),
    df['variantUnits'].str.contains("cl",regex=False) | df['variantUnits'].str.contains("cm",regex=False),
    df['variantUnits'].str.contains("l",regex=False)
     | df['variantUnits'].str.contains("kq",regex=False)
     | df['variantUnits'].str.contains("kg",regex=False)
     | df['variantUnits'].str.contains("k",regex=False)]



  choices = [df['productPrice']/(df['variantQ1']*df['variantQ2']),df['productPrice']/(df['variantQ1']*df['variantQ2']*100), df['productPrice']/(df['variantQ1']*df['variantQ2']*1000)]
  df['productPriceQuantityRatio'] = np.select(conditions, choices, default=df['productPrice']/df['variantQuantity'])

  conditions = [df['variantUnits'].str.contains("cl",regex=False) | df['variantUnits'].str.contains("l",regex=False),
                df['variantUnits'].str.contains("m",regex=False) | df['variantUnits'].str.contains("mm",regex=False),
                df['variantUnits'].str.contains("kg",regex=False) | df['variantUnits'].str.contains("kq",regex=False) ]

  choices = ['ml','mm','g']

  df['variantUnits'] = np.select(conditions, choices, default='ud')

  df.drop(['variantQ1', 'variantQ2','variantQuantity'], axis=1, inplace=True)


  return df

#----------------------------------------------------------------------------------------------------------------
# This function gets the number of products in a cart per category
#----------------------------------------------------------------------------------------------------------------

def get_prods_quantity_per_category(df, top_cats):

    #print(len(df))
    per_cat=df.groupby(['clientId','category'])['productQuantity'].sum().reset_index()
    cats=list(top_cats['category'])
    per_cat_pivot = pd.pivot_table(per_cat[per_cat.category.isin(cats)], index=['clientId'], columns='category',values='productQuantity',
                            aggfunc=sum, fill_value=0.0).reset_index()
    #perCatPivot = pd.pivot_table(perCat, index=['clientId','parentTransactionId'], columns='category',
    #                         aggfunc=sum, fill_value=0).reset_index()
    #print("*******************")
    #print(perCatPivot.head())
    #print("*******************")
    #for c  in list(perCatPivot):
    #    print(c[1])
    #    if (not(c[1] in cats)):
    #        perCatPivot=perCatPivot.drop(columns={c})


    for c in cats:
      #varname = ('productQuantity', c)
      if c not in list(per_cat_pivot):
          per_cat_pivot[c] = 0
      per_cat_pivot=per_cat_pivot.rename(columns={c: "prodQty_"+c})


    return per_cat_pivot

#----------------------------------------------------------------------------------------------------------------
# This function gets the revenue per category in a cart
#----------------------------------------------------------------------------------------------------------------

def get_cart_revenue_per_category(df,top_cats):

    per_cat=df.groupby(['clientId','category'])['productRevenue'].sum().reset_index()
    cats=list(top_cats['category'])

    per_cat_pivot = pd.pivot_table(per_cat[per_cat.category.isin(cats)], index=['clientId'], columns='category',values='productRevenue',
                             aggfunc=sum, fill_value=0.0).reset_index()


    for c in cats:

      if c not in list(per_cat_pivot):
        per_cat_pivot[c] = 0
      per_cat_pivot=per_cat_pivot.rename(columns={c: "prodRev_"+c})


    return per_cat_pivot

#----------------------------------------------------------------------------------------------------------------
# This function replaces the wrong characters for BQ column names
#----------------------------------------------------------------------------------------------------------------
def sanitise_string(v):

    if ("`" in v):
       v=v.replace("`", "_")
    if ("''" in v):
       v=v.replace("''", "_")
    if (" " in v):
       v=v.replace(" ", "_")
    if ("&" in v):
       v=v.replace("&", "_and_")
    if ("-" in v):
       v=v.replace("-", "_")
    if (is_number(v)):
        v="_"+v

    return v
#----------------------------------------------------------------------------------------------------------------
# This function gets the number of products of each of the top brands
#----------------------------------------------------------------------------------------------------------------
def get_cart_products_per_top_brand(df,top_brands):

    per_brand=df.groupby(['clientId','productBrand'])['productQuantity'].sum().reset_index()
    brands=list(top_brands['productBrand'])
    per_brand['productBrand'] = df.productBrand.astype(str)

    pivot = pd.pivot_table(per_brand[per_brand.productBrand.isin(brands)], index=['clientId'], columns='productBrand', values='productQuantity',
                         aggfunc=sum, fill_value=0.0).reset_index()

    col_list=list(pivot)
    for c in brands:
      if c not in col_list:
        pivot[c] = 0

    for i,v in enumerate(col_list):
        #print(v)
        pivot=pivot.rename(columns={v: sanitise_string(v)})

    return pivot


#----------------------------------------------------------------------------------------------------------------
# This function gets the average price per quantity and unit for each category, overall purchases
#----------------------------------------------------------------------------------------------------------------
def get_products_over_avg_price_per_category(df):

    per_cat=df.groupby(['category','variantUnits'])['productPriceQuantityRatio'].mean().reset_index()

    return per_cat

#----------------------------------------------------------------------------------------------------------------
# This function gets the number of products in a cart
#----------------------------------------------------------------------------------------------------------------
def get_cart_number_products(df):

    res=df.groupby(['clientId'])['productQuantity'].sum().reset_index()

    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets the differnt products in a cart
#----------------------------------------------------------------------------------------------------------------
def get_cart_different_products(df):

    res=df.groupby(['clientId'])['productSKU'].count().reset_index()
    return res


#----------------------------------------------------------------------------------------------------------------
# This function gets the total revenue of a cart
#----------------------------------------------------------------------------------------------------------------
def get_cart_revenue(df):

    res=df.groupby(['clientId'])['productRevenue'].sum().reset_index()
    res=res.rename(columns={'productRevenue': 'cartRevenue'})

    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets number of products over the average product price in the category, based on ml,g,mm or unit
# ratio
#----------------------------------------------------------------------------------------------------------------
def get_cart_products_above_avg_unit_price_per_category(df, avgs, top_cats):

    #perCat=df.groupby(['parentTransactionId','category','variantUnits'])['productPriceQuantityRatio'].reset_index()
    #perCat=df.merge(avgs,how='left',left_on='variantUnits,category', right_on='variantUnits,category')
    cats=list(top_cats['category'])

    per_cat = pd.merge(df,avgs,how='left', on=['category','variantUnits'])
    per_cat = per_cat[(per_cat.productPriceQuantityRatio_x > per_cat.productPriceQuantityRatio_y)].groupby(['clientId','category'])['productPriceQuantityRatio_x'].count().reset_index()

    per_cat = pd.pivot_table(per_cat[per_cat.category.isin(cats)]
                            , index='clientId'
                            , columns='category'
                            , values='productPriceQuantityRatio_x'
                            , aggfunc=sum
                            , fill_value=0.0).reset_index()
    #print("list(perCat)")
    #print(list(perCat))
    for c in cats:
      #varname = ('productPriceQuantityRatio_x', c)
      if c not in list(per_cat):
        per_cat[c] = 0
      per_cat=per_cat.rename(columns={c: "prodsAboveAvg_"+c})

    #perCat = perCat['parentTransactionId', 'category','productPriceQuantityRatio_x']



    return per_cat

#----------------------------------------------------------------------------------------------------------------
# This function gets the total revenue for the customer in the cohort period
#----------------------------------------------------------------------------------------------------------------
def get_customer_revenue_in_period(df):
    res=df.groupby('clientId')['productRevenue'].sum().reset_index()
    res=res.rename(columns={'productRevenue':'customerValue'})
    return res

#----------------------------------------------------------------------------------------------------------------
# This function gets per cart the number of discounts used and the total amount of the discounts
#----------------------------------------------------------------------------------------------------------------
def get_discounts_per_cart(df):
    res1=df.groupby(['clientId','parentTransactionId'])['cmValue'].mean().reset_index()
    res1=res1.rename(columns={'cmValue': 'couponsAmount'})
    res1['couponsAmount'].map(lambda x: x/1000000)
    res2=df.groupby(['clientId','parentTransactionId'])['cmValue'].count().reset_index()
    res2=res2.rename(columns={'cmValue': 'totalCoupons'})

    return pd.merge(res1,res2,how='left', on='clientId').reset_index()



#----------------------------------------------------------------------------------------------------------------
# This function gets the first transactions without outliers
#----------------------------------------------------------------------------------------------------------------
def get_first_transactions(df):
    res=df[df.date==df.acquisitionDate]

    #cleaning the outliers with more than 1 transaction as first ever transaction
    f=res.groupby('clientId')['parentTransactionId'].nunique().reset_index()
    exclusion_list=list(f[f.parentTransactionId>1].clientId)
    res=res[~res.clientId.isin(exclusion_list)]

    return res

#----------------------------------------------------------------------------------------------------------------
# This function cleans the customers not fitting into the time window
#----------------------------------------------------------------------------------------------------------------
def clean_customers_out_of_window(df):

    max_date=datetime.datetime.strptime(df.date.max(), '%Y%m%d')
    is_out = lambda x: (datetime.datetime.strptime(x, '%Y%m%d')+datetime.timedelta(days=NUM_DAYS)) > max_date
    df=df[df.acquisitionDate.map(is_out)==False]

    #print(df.acquisitionDate.max())
    return df


#----------------------------------------------------------------------------------------------------------------
# This function gets the main stats for the customer
#----------------------------------------------------------------------------------------------------------------
def get_main_stats(df):
    res=df.groupby([
          'clientId'
          ,'acquisitionDate'
          ]).agg({'time': 'first'
                 ,'timeOnSite': 'first'
                 ,'weekday':'first'
                 ,'dayMoment':'first'
                 ,'deviceCategory': 'first'
                 ,'mobileDeviceBranding': 'first'
                 ,'operatingSystem': 'first'
                 ,'browser': 'first'
                 ,'city':'first'
#                 ,'dataHash':'first'
                 }).reset_index()

    res['browser']=res['browser'].fillna("Other")
    res['mobileDeviceBranding']=res['mobileDeviceBranding'].fillna("Other")
    res['city']=res['city'].fillna("Other")

    #res['deviceCategory']=res.deviceCategory.astype(str)
    #res['mobileDeviceBranding']=res.deviceCategory.astype(str)
    #res['operatingSystem']=res.deviceCategory.astype(str)
    #res['browser']=res.deviceCategory.astype(str)
    return res

#----------------------------------------------------------------------------------------------------------------
# This function calculates all the metrics and merge all the subtables
#----------------------------------------------------------------------------------------------------------------
def merge_them_all(df):
    #Full list which are not discounts
    full_not_discount_df=df[df.cmIndex>3]

    #First transactions
    first_transactions_df=get_first_transactions(df)
    #print("firstTransactionsdf['clientId'].unique()")
    #print(len(firstTransactionsdf['clientId'].unique()))


    #First Transactions which are not discounts
    first_tr_not_dis_df=first_transactions_df[first_transactions_df.cmIndex>3]
    #print("firstTrNotDisdf['clientId']")
    #print(len(firstTrNotDisdf['clientId']))
    #print("firstTrNotDisdf['clientId'].unique()")
    #print(len(firstTrNotDisdf['clientId'].unique()))

    #First Transactions which are discounts
    first_tr_dis_df=first_transactions_df[first_transactions_df.cmIndex==3]
    first_tr_dis_df=first_tr_dis_df.drop(['deviceCategory','mobileDeviceBranding','operatingSystem','browser'],axis=1).reset_index()


    #print(df[df.clientId =='1000015364.1571176950'])
    #print(fullNotDiscountdf[fullNotDiscountdf.clientId =='1000015364.1571176950'])

    #calculations requiring the full list of transactions which are not discounts
    #=================================================================
    customer_value=get_customer_revenue_in_period(full_not_discount_df)
    #print("customerValue")
    #print("--------------------")
    #print(customerValue.head())

    top_cats_per_revenue=get_top_cats_per_revenue(full_not_discount_df,NUM_CATEGORIES)
    #print("topCatsPerRevenue")
    #print("--------------------")
    #print(topCatsPerRevenue.head())


    #topCatsPerQuantity=getTopCatsPerQuantity(fullNotDiscountdf,NUM_CATEGORIES)
    #print("topCatsPerQuantity")
    #print("--------------------")
    #print(topCatsPerQuantity.head())


    top_brands_per_revenue=get_top_brands(full_not_discount_df,NUM_BRANDS)
    #print("topBrands")
    #print("--------------------")
    #print(topBrands.head())



    product_price_avgs=get_products_over_avg_price_per_category(full_not_discount_df)
    #print("product_price_avgs")
    #print("--------------------")
    #print(productPriceAvgs.head())


    #calculations requiring the first transactions which are discounts
    #=================================================================
    cart_coupons=get_discounts_per_cart(first_tr_dis_df)
    #print("cartCoupons")
    #print("--------------------")
    #print(cartCoupons.head())


    #calculations requiring the first transactions which are no discounts
    #=================================================================
    main_stats=get_main_stats(first_tr_not_dis_df)
    first_tr_not_dis_df=first_tr_not_dis_df.drop(['deviceCategory','mobileDeviceBranding','operatingSystem','browser'],axis=1).reset_index()


    cart_revenue=get_cart_revenue(first_tr_not_dis_df)
    #print("cartRevenue")
    #print(cartRevenue.head())
    #print("--------------------")

    cart_number_of_products=get_cart_number_products(first_tr_not_dis_df)
    #print("cartNumberOfProducts")
    #print(cartNumberOfProducts.head())
    #print("--------------------")

    cart_different_products=get_cart_different_products(first_tr_not_dis_df)
    #print("cartDifferentProducst")
    #print(cartDifferentProducts.head())
    #print("--------------------")

    cart_prod_qty_per_cat=get_prods_quantity_per_category(first_tr_not_dis_df,top_cats_per_revenue)
    #print("cartProdQtyPerCat")
    #print(cartProdQtyPerCat.head())
    #print("--------------------")


    cart_prod_rev_per_cat=get_cart_revenue_per_category(first_tr_not_dis_df,top_cats_per_revenue)
    #print("cartProdRevPerCat")
    #print(cartProdRevPerCat.head())
    #print("--------------------")


    cart_prod_qty_per_brand=get_cart_products_per_top_brand(first_tr_not_dis_df,top_brands_per_revenue)
    #print("cartProdQtyPerBrand")
    #print(cartProdQtyPerBrand.head())
    #print("--------------------")



    cart_products_over_avg_price_per_category=get_cart_products_above_avg_unit_price_per_category(first_tr_not_dis_df
                                                                                                  , product_price_avgs
                                                                                                  , top_cats_per_revenue)
    #print("cartProductsOverAvgPricePerCategory")
    #print(cartProductsOverAvgPricePerCategory.head())
    #print("--------------------")



    res=pd.merge(main_stats,cart_prod_qty_per_brand,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_prod_rev_per_cat,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_prod_qty_per_cat,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_different_products,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_number_of_products,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_revenue,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_coupons,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,cart_products_over_avg_price_per_category,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res=pd.merge(res,customer_value,how='left',on='clientId',validate="one_to_one").fillna(value=0.0)
    res['customerValue']=res['customerValue']-res['cartRevenue']
    #res['logCustomerValue']=res['customerValue'].apply(lambda x: math.log(x))

    res=res.drop(['parentTransactionId_x','parentTransactionId_y','index'],axis=1)


    #res.head()
    top_cats_per_revenue['type']="category";
    top_cats_per_revenue=top_cats_per_revenue.rename(columns={"category": "value"})

    top_brands_per_revenue['type']="brand";
    top_brands_per_revenue=top_brands_per_revenue.rename(columns={"productBrand": "value"})
    feature_list=pd.concat([top_cats_per_revenue,top_brands_per_revenue])

    return res,feature_list,product_price_avgs

#----------------------------------------------------------------------------------------------------------------
# This function returns the label according to the value and Threshold.
#----------------------------------------------------------------------------------------------------------------
def two_buckets(value):
    if (value<REVENUE_THRESHOLD):
        return 'A'
    else:
        return 'B'

#----------------------------------------------------------------------------------------------------------------
# This function labels the customers in 2 classes: A < Threshold & B >= Threshold.
#----------------------------------------------------------------------------------------------------------------
def bucketize(cdf):
    cdf['customerBucketValue']=cdf.customerValue.apply(lambda x: two_buckets(x))
    return cdf
#----------------------------------------------------------------------------------------------------------------
# This function removes as many 0 LTV values as required to balance non 0 ones.
#----------------------------------------------------------------------------------------------------------------
def balance_data_set(df):

    cdf=df.copy()
    print(len(df))
    #print(customerDatadf.columns)
    count_a=len(cdf[cdf.customerBucketValue=='A'])
    count_b=len(cdf[cdf.customerBucketValue=='B'])

    print(count_a)
    print(count_b)
    diff=count_a-count_b
    cdf=cdf.sample(frac=1).reset_index(drop=True)
    cdf=cdf.sample(frac=1).reset_index(drop=True)


    index_list_to_delete = cdf[ (cdf['customerBucketValue']=='A')].index
    #indexListToDelete=indexListToDelete.sample(frac=1).reset_index(drop=True)
    #random.shuffle(indexListToDelete)
    #=indexListToDelete.head(countB-countA)


    cdf=cdf.drop(index_list_to_delete[:diff])

    return cdf


#----------------------------------------------------------------------------------------------------------------
# This function writes the metadata to bigquery
#----------------------------------------------------------------------------------------------------------------
def write_metadata_to_bigquery():

    df=pd.DataFrame(data={'latest_model': [CURRENT_DATE]})

    col_list=list(df)

    dataframe=df[col_list]
    client = bigquery.Client()

    table_id = INOUT_BQ_METADATA_TABLE

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = [
            bigquery.SchemaField('current_model','STRING',mode='REQUIRED')
            ]



    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

#----------------------------------------------------------------------------------------------------------------
# This function writes the feature list to bigquery
#----------------------------------------------------------------------------------------------------------------
def write_product_price_avgs_to_bigquery(df):


    #dataframe=df
    #print(df)
    # Construct a BigQuery client object.
    col_list=list(df)

    dataframe=df[col_list]

    #print(list(dataframe))
    #print(dataframe.dtypes)

    client = bigquery.Client()


    table_id = OUTPUT_BQ_PRICE_AVGS_TABLE

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = [
            bigquery.SchemaField('category','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('variantUnits','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('productPriceQuantityRatio','FLOAT',mode='REQUIRED')
            ]



    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )




#----------------------------------------------------------------------------------------------------------------
# This function writes the feature list to bigquery
#----------------------------------------------------------------------------------------------------------------
def write_feature_list_to_bigquery(df):

    df=df.drop(columns= ['index'])
    df=df.reset_index(drop=True)
    df['value']=df['value'].apply(lambda x: sanitise_string(x))

    #dataframe=df
    #print(df)
    # Construct a BigQuery client object.
    col_list=list(df)

    dataframe=df[col_list]

    #print(list(dataframe))
    #print(dataframe.dtypes)

    client = bigquery.Client()



    table_id = OUTPUT_BQ_FEATURES_TABLE

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = [
            bigquery.SchemaField('type','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('value','STRING',mode='REQUIRED')
            ]



    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


#----------------------------------------------------------------------------------------------------------------
# This function writes the dataframe to bigquery
#----------------------------------------------------------------------------------------------------------------
def write_data_to_bigquery(df):
    dataframe=df
    #print(featureList)
    # Construct a BigQuery client object.
    col_list=list(dataframe)


    dataframe=df[col_list]
    dataframe=dataframe.drop(columns={'mobileDeviceBranding'})

    #print(list(dataframe))
    #print(dataframe.dtypes)

    client = bigquery.Client()



    table_id = OUTPUT_BQ_DATA_TABLE

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = [
            bigquery.SchemaField('clientId','STRING',mode='REQUIRED')
            #,bigquery.SchemaField('date','INTEGER',mode='REQUIRED')
            #,bigquery.SchemaField('time','INTEGER',mode='NULLABLE')
            #,bigquery.SchemaField('timeOnSite','INTEGER',mode='NULLABLE')
            ,bigquery.SchemaField('weekday','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('dayMoment','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('deviceCategory','STRING',mode='REQUIRED')
            ,bigquery.SchemaField('browser','STRING',mode='REQUIRED')
            ]



    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
#----------------------------------------------------------------------------------------------------------------
# This function writes the dataframe and features to bigquery
#----------------------------------------------------------------------------------------------------------------
def write_to_bigquery(data_df,features_df,product_price_avgs):
    write_feature_list_to_bigquery(features_df)
    write_product_price_avgs_to_bigquery(product_price_avgs)
    write_data_to_bigquery(data_df)
    write_metadata_to_bigquery()


#----------------------------------------------------------------------------------------------------------------
# This function gets the categories and brands used for training the model
#----------------------------------------------------------------------------------------------------------------

def load_categories_and_brands_from_bq():
    query = """
    SELECT
       *
      FROM
        """ + str(BQ_FEATURES_TABLE)

    df = bigquery.Client().query(query).to_dataframe()
    top_cats=df[df.type=='category']
    top_brands=df[df.type=='brand']

    return top_cats,top_brands

#----------------------------------------------------------------------------------------------------------------
# This function gets the main stats for the customer
#----------------------------------------------------------------------------------------------------------------

def load_data_from_bq():
    query = """
    SELECT
    date as date
    ,acquisitionDate as acquisitionDate
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
    FROM {0} 
    WHERE 1=1
          AND PARSE_DATETIME('%E4Y%m%d', date) <= PARSE_DATETIME('%E4Y%m%d', '{1}')
    """

    #and DATETIME_DIFF(
    #        PARSE_DATETIME('%E4Y%m%d', date),
    #        PARSE_DATETIME('%E4Y%m%d', acquisitionDate),
    #        DAY) <=  {2}"""


    query=query.format(INPUT_BQ_TRANSACTIONS_TABLE,END_DATE)

    #train_query = "SELECT *,ABS(MOD(FARM_FINGERPRINT(clientId), "+str(DATASET_MOD_SPLIT)+")) as dataHash FROM (" + query + ") WHERE RAND() <= "+str(AMOUNT_OF_DATA)
    #print(train_query)


    job_config = bigquery.job.QueryJobConfig()


    train_df = bigquery.Client().query(query,job_config=job_config).to_dataframe()

    return train_df


#----------------------------------------------------------------------------------------------------------------
# This function gets the main stats for the customer
#----------------------------------------------------------------------------------------------------------------


def main():



    metadata_df=load_metadata()

    global CURRENT_DATE
    CURRENT_DATE=str(metadata_df['export_date'][0])

    global INPUT_BQ_TRANSACTIONS_TABLE
    INPUT_BQ_TRANSACTIONS_TABLE=BQ_TABLE_PREFIX+"transactions_"+CURRENT_DATE

    global OUTPUT_BQ_CLIENTS_TABLE
    OUTPUT_BQ_CLIENTS_TABLE=BQ_TABLE_PREFIX+"processed_clients"

    global OUTPUT_BQ_TABLE_PREFIX
    OUTPUT_BQ_TABLE_PREFIX=BQ_TABLE_PREFIX+"training_data"

    global OUTPUT_BQ_DATA_TABLE
    OUTPUT_BQ_DATA_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_"+CURRENT_DATE

    global OUTPUT_BQ_FEATURES_TABLE
    OUTPUT_BQ_FEATURES_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_features_"+CURRENT_DATE

    global OUTPUT_BQ_PRICE_AVGS_TABLE
    OUTPUT_BQ_PRICE_AVGS_TABLE=OUTPUT_BQ_TABLE_PREFIX+"_price_avgs_"+CURRENT_DATE



    df = load_data_from_bq()
    #final_df=df.copy()
    final_df=df
    final_df = clean_customers_out_of_window(final_df)
    final_df=preprocess(final_df)
    final_df,feature_list,product_price_avgs = merge_them_all(final_df)
    final_df = bucketize(final_df)
    final_df = balance_data_set(final_df)
    write_to_bigquery(final_df,feature_list,product_price_avgs)

#final_df['customerValue'].describe()
#final_df['customerBucketValue'].describe()
#write_to_bigquery(final_df,feature_list,product_price_avgs)
#final_df['customerValue'].describe()
#final_df[final_df.customerBucketValue=='B']['customerValue'].describe()
#final_df['clientId'].describe()
#final_df['acquisitionDate'].describe()
#final_df['customerValue'].describe()
#final_df[final_df.customerBucketValue=='B']['customerValue'].describe()
#final_df['clientId'].describe()
main()
