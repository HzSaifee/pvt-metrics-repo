SET spark.sql.sources.partitionOverwriteMode=DYNAMIC;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE cdt.MAIN_TABLE_NAME_TO_SET   -- MAIN_TABLE_NAME_TO_SET to be set in the Python Script
PARTITION (month)
(
    SELECT
        implementation_type_name,
        module,
        tools,
        environment,
        cast(composite_type AS BOOLEAN),
        cast(count AS BIGINT),
        cast(avg_transformation_time AS FLOAT),
        cast(max_trans_time AS BIGINT),
        cast(90_perc_trans_time AS FLOAT),
        cast(95_perc_trans_time AS FLOAT),
        cast(avg_ws_time AS FLOAT),
        cast(max_ws_time AS BIGINT),
        cast(90_perc_ws_time AS FLOAT),
        cast(95_perc_ws_time AS FLOAT),
        cast(avg_total_time AS FLOAT),
        cast(max_total_time AS BIGINT),
        cast(90_perc_total_time AS FLOAT),
        cast(95_perc_total_time AS FLOAT),
        cast(sum_instance_count AS BIGINT),
        cast(max_instance_count AS BIGINT),
        cast(90_perc_instance_count AS FLOAT),
        cast(95_perc_instance_count AS FLOAT),
        cast(mean_trans_time_per_instance AS FLOAT),
        cast(std_trans_time_per_instance AS FLOAT),
        cast(mean_ws_time_per_instance AS FLOAT),
        cast(std_ws_time_per_instance AS FLOAT),
        cast(mean_tot_time_per_instance AS FLOAT),
        cast(std_tot_time_per_instance AS FLOAT),
        cast(month AS TIMESTAMP)
    FROM cdt.TEMP_TABLE_NAME_TO_SET    -- TEMP_TABLE_NAME_TO_SET to be set in the Python Script
);