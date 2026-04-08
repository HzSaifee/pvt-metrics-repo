SELECT DISTINCT
  "bt_opportunity"."accountid"
FROM "bz_sales_data"."bt_opportunity" "bt_opportunity"
WHERE CAST("bt_opportunity"."external_selected_package_s__c" AS TEXT) IN (
    'Workday GO Deployment',
    'Workday GO Frontline Worker',
    'Workday GO Global Payroll',
    'Workday GO for HR',
    'Workday Go for Finance - LDP',
    'Workday Go for HR - LDP',
    'Workday Go for HR - LDP;Workday GO for HR',
    'Workday Go for HR - LDP;Workday GO for HR;Workday Go Global Payroll;Workday Go Deployment',
    'Workday Go for HR - LDP;Workday Go for Finance - LDP'
)