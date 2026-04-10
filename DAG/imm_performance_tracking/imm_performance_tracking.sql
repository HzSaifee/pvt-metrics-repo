SELECT t1.implementation_type_name, t1.month, t1.tools, t1.environment,

      t1.count,

      t1.avg_transformation_time, t1.max_trans_time,
      t2.90_perc_trans_time, t2.95_perc_trans_time,

      t1.avg_ws_time, t1.max_ws_time,
      t2.90_perc_ws_time, t2.95_perc_ws_time,

      t1.avg_total_time, t1.max_total_time,
      t2.90_perc_total_time, t2.95_perc_total_time,

      t1.sum_instance_count, t1.max_instance_count,
      t2.90_perc_instance_count, t2.95_perc_instance_count

FROM (SELECT implementation_type_name                             AS implementation_type_name,
            date_trunc('month', CAST(wd_event_date AS TIMESTAMP)) AS month,
            client_id                                             AS tools,
            wd_env_type                                           AS environment,

            COUNT(*)                                              AS count,

            AVG(transformation_time)                              AS avg_transformation_time,
            MAX(transformation_time)                              AS max_trans_time,

            AVG(ws_time)                                          AS avg_ws_time,
            MAX(ws_time)                                          AS max_ws_time,

            AVG(total_time)                                       AS avg_total_time,
            MAX(total_time)                                       AS max_total_time,

            SUM(instance_count)                                   AS sum_instance_count,
            MAX(instance_count)                                   AS max_instance_count
      FROM goku.prime_metrics
      WHERE CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
      GROUP BY implementation_type_name, month, tools, environment
      ORDER BY month DESC, count, implementation_type_name, tools, environment)   AS t1,
      (SELECT implementation_type_name                            AS implementation_type_name,
            date_trunc('month', CAST(wd_event_date AS TIMESTAMP)) AS month,

            PERCENTILE(transformation_time, 0.90)                 AS 90_perc_trans_time,
            PERCENTILE(transformation_time, 0.95)                 AS 95_perc_trans_time,

            PERCENTILE(ws_time, 0.90)                             AS 90_perc_ws_time,
            PERCENTILE(ws_time, 0.95)                             AS 95_perc_ws_time,

            PERCENTILE(total_time, 0.90)                          AS 90_perc_total_time,
            PERCENTILE(total_time, 0.95)                          AS 95_perc_total_time,

            PERCENTILE(instance_count, 0.90)                      AS 90_perc_instance_count,
            PERCENTILE(instance_count, 0.95)                      AS 95_perc_instance_count
      FROM goku.prime_metrics
      WHERE CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
      GROUP BY month, implementation_type_name
      ORDER BY month DESC, implementation_type_name)  AS t2

WHERE t1.implementation_type_name = t2.implementation_type_name
  AND t1.month = t2.month
ORDER BY t1.month DESC, t1.implementation_type_name, t1.tools, t1.environment