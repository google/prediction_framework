#!/bin/bash
# SAMPLE QUERY TO EXTRACT FROM GA data
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
#EXTRACT_ALL_TRANSACTIONS_QUERY='with
#  transactions as (
#
#      SELECT
#
#       date as date
#      ,visitStartTime as time
#      ,userId as userId
#      ,clientId as clientId
#      ,h.hitNumber as hitNumber
#      ,totals.timeOnSite as timeOnSite
#      ,totals.transactions as totalsTransactions
#      ,totals.transactionRevenue as totalsTransactionRevenue
#      ,device.deviceCategory as deviceCategory
#      ,device.mobileDeviceBranding as mobileDeviceBranding
#      ,device.operatingSystem as operatingSystem
#      ,device.browser as browser
#      ,cd.value as parentTransactionId
#      ,h.transaction.transactionId as transactionId
#      ,h.transaction.transactionRevenue as transactionRevenue
#      ,h.transaction.transactionShipping
#      ,h.transaction.transactionCoupon
#      ,p.productSKU as productSKU
#      ,p.v2ProductName as productName
#      ,p.v2ProductCategory as productCategory
#      ,SPLIT(SPLIT(p.v2ProductCategory, \"/\")[safe_ORDINAL(1)],\"-\")[safe_ORDINAL(1)] AS catL1
#      ,SPLIT(SPLIT(p.v2ProductCategory, \"/\")[safe_ORDINAL(2)],\"-\")[safe_ORDINAL(1)] AS catL2
#      ,SPLIT(SPLIT(p.v2ProductCategory, \"/\")[safe_ORDINAL(3)],\"-\")[safe_ORDINAL(1)] AS catL3
#      ,p.productVariant as productVariant
#      ,p.productQuantity  as productQuantity
#      ,p.productBrand as productBrand
#      ,p.productPrice as productPrice
#      ,p.productRevenue as productRevenue
#      ,p.productCouponCode as productCouponCode
#      ,cm
#      ,cd
#      ,cd.value != h.transaction.transactionId as isSub
#      ,geoNetwork.city as city
#      ,trafficSource.adwordsClickInfo.gclId as gclid
#      ,trafficSource.adwordsClickInfo.campaignId as campaignId
#      ,channelgrouping as channelgrouping
#
#     FROM
#      `$TABLE` s
#      JOIN
#        UNNEST(hits) AS h
#      JOIN
#        UNNEST(product) AS p
#      JOIN
#        UNNEST(h.customMetrics) AS cm
#      JOIN
#        UNNEST(h.customDimensions) AS cd
#
#      WHERE 1=1
#
#        AND _TABLE_SUFFIX BETWEEN
#          FORMAT_DATE(\"%Y%m%d\", DATE_SUB(@run_date, INTERVAL 2 DAY)) AND
#          FORMAT_DATE(\"%Y%m%d\", DATE_SUB(@run_date, INTERVAL 1 DAY))
#        and h.sourcePropertyInfo.sourcePropertyTrackingId = \"XXXXXX\"
#        and cd.index=YYY
#
#
#
#  )
#  SELECT * FROM transactions;'

EXTRACT_ALL_TRANSACTIONS_QUERY='Select * from `$TABLE` limit 10000;'