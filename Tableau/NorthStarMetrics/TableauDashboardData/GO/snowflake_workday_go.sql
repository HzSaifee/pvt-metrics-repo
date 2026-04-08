SELECT DISTINCT
  "OPPORTUNITY"."ACCOUNTID"
FROM "BASE_PROD"."SALESFORCE"."OPPORTUNITY" "OPPORTUNITY"
WHERE CAST("OPPORTUNITY"."EXTERNAL_SELECTED_PACKAGE_S__C" AS TEXT) IN (
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