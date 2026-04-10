SELECT
    tenant_n as tenant,
    common_request_id,
    job_definition,
    job_definition_name,
    status,
    summary_duration/1000 as total_time,
    number_of_instances,
    wd_env_logical,
    wd_env_type,
    CAST(wd_event_date as TIMESTAMP) as wd_event_date
FROM swh.job_summary_stats
WHERE
    wd_event_date IS NOT NULL
         AND CAST(wd_event_date as TIMESTAMP) >= CAST(date_add('day', -90, current_date) AS TIMESTAMP) and wd_event_date like '2%'
         AND job_definition in ('4608$3275','4608$3265','4608$3316','4608$3356','4608$3544')
         AND summary_type = 'JOB'
         AND wd_env_type in ('IMPL','SANDBOX','PROD')