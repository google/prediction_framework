/*
WITH
  transactions AS (
  SELECT
    date AS date,
    visitStartTime AS time,
    userId AS userId,
    clientId AS clientId,
    h.hitNumber AS hitNumber,
    totals.timeOnSite AS timeOnSite,
    totals.transactions AS totalsTransactions,
    totals.transactionRevenue AS totalsTransactionRevenue,
    device.deviceCategory AS deviceCategory,
    device.mobileDeviceBranding AS mobileDeviceBranding,
    device.operatingSystem AS operatingSystem,
    device.browser AS browser,
    cd.value AS parentTransactionId,
    h.TRANSACTION.transactionId AS transactionId,
    h.TRANSACTION.transactionRevenue AS transactionRevenue,
    h.TRANSACTION.transactionShipping,
    h.TRANSACTION.transactionCoupon,
    p.productSKU AS productSKU,
    p.v2ProductName AS productName,
    p.v2ProductCategory AS productCategory,
    SPLIT(SPLIT(p.v2ProductCategory, "/")[safe_ORDINAL(1)],"-")[safe_ORDINAL(1)] AS catL1,
    SPLIT(SPLIT(p.v2ProductCategory, "/")[safe_ORDINAL(2)],"-")[safe_ORDINAL(1)] AS catL2,
    SPLIT(SPLIT(p.v2ProductCategory, "/")[safe_ORDINAL(3)],"-")[safe_ORDINAL(1)] AS catL3,
    p.productVariant AS productVariant,
    p.productQuantity AS productQuantity,
    p.productBrand AS productBrand,
    p.productPrice AS productPrice,
    p.productRevenue AS productRevenue,
    p.productCouponCode AS productCouponCode,
    cm,
    cd,
    cd.value != h.TRANSACTION.transactionId AS isSub,
    geoNetwork.city AS city,
    trafficSource.adwordsClickInfo.gclId AS gclid,
    trafficSource.adwordsClickInfo.campaignId AS campaignId,
    channelgrouping AS channelgrouping
  FROM
    `$TABLE` s
  JOIN
    UNNEST(hits) AS h
  JOIN
    UNNEST(product) AS p
  JOIN
    UNNEST(h.customMetrics) AS cm
  JOIN
    UNNEST(h.customDimensions) AS cd
  WHERE
    1=1
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(@run_date, INTERVAL 2 DAY))
    AND FORMAT_DATE("%Y%m%d", DATE_SUB(@run_date, INTERVAL 1 DAY))
    AND h.sourcePropertyInfo.sourcePropertyTrackingId = "XXXXXX"
    AND cd.index=1234 )
SELECT
  *
FROM
  transactions;
*/
Select * from `$TABLE` limit 10000;