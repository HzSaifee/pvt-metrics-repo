SELECT DISTINCT
  "bt_opportunity"."accountid"
FROM "bz_sales_data"."bt_opportunity" "bt_opportunity"
WHERE LOWER(CAST("bt_opportunity"."external_selected_package_s__c" AS TEXT)) LIKE '%workday go%'