CREATE TABLE IF NOT EXISTS cdt.MAIN_TABLE_NAME_TO_SET ( -- MAIN_TABLE_NAME_TO_SET to be set in the Python Script
    implementation_type_name            STRING  COMMENT 'Implementation Type',
    module                              STRING  COMMENT 'Module assigned to the Implementation Type',
    tools                               STRING  COMMENT 'Tools that ran the Implementation Type',
    environment                         STRING  COMMENT 'Environment on which the Implementation Type ran',
    count                               BIGINT  COMMENT 'Number of times the Implementation Type ran on the specific Tool & Env for the Month',
    avg_transformation_time             FLOAT   COMMENT 'Average Transformation Time taken for Implementation Type on the specific Tool & Env for the Month',
    max_trans_time                      BIGINT  COMMENT 'Maximum Transformation Time over all runs for Implementation Type on the specific Tool & Env for the Month',
    90_perc_trans_time                  FLOAT   COMMENT '90 Percentile of Transformation Time over all runs for Implementation Type on the specific Tool & Env for the Month',
    95_perc_trans_time                  FLOAT   COMMENT '95 Percentile of Transformation Time over all runs for Implementation Type on the specific Tool & Env for the Month',
    avg_ws_time                         FLOAT   COMMENT 'Average Web Service Time taken for Implementation Type on the specific Tool & Env for the Month',
    max_ws_time                         BIGINT  COMMENT 'Maximum Web Service Time over all runs for Implementation Type on the specific Tool & Env for the Month',
    90_perc_ws_time                     FLOAT   COMMENT '90 Percentile of Web Service Time over all runs for Implementation Type for the Month',
    95_perc_ws_time                     FLOAT   COMMENT '95 Percentile of Transformation Time over all runs for Implementation Type for the Month',
    avg_total_time                      FLOAT   COMMENT 'Average Total Time taken for Implementation Type on the specific Tool & Env for the Month',
    max_total_time                      BIGINT  COMMENT 'Maximum Total Time over all runs for Implementation Type on the specific Tool & Env for the Month',
    90_perc_total_time                  FLOAT   COMMENT '90 Percentile of Total Time over all runs for Implementation Type for the Month',
    95_perc_total_time                  FLOAT   COMMENT '95 Percentile of Total Time over all runs for Implementation Type for the Month',
    sum_instance_count                  BIGINT  COMMENT 'Total Instance Count ran of Implementation Type on the specific Tool & Env for the Month',
    max_instance_count                  BIGINT  COMMENT 'Maximum Instance Count of Implementation Type on the specific Tool & Env for the Month',
    90_perc_instance_count              FLOAT   COMMENT '90 Percentile of Instance Count of Implementation Type for the Month',
    95_perc_instance_count              FLOAT   COMMENT '95 Percentile of Instance Count of Implementation Type for the Month',
    mean_trans_time_per_instance        FLOAT   COMMENT 'Mean Transformation Time Per Instance for Implementation Type over complete period of data fetched',
    std_trans_time_per_instance         FLOAT   COMMENT 'Standard Deviation of Transformation Time Per Instance for Implementation Type over complete period of data fetched',
    mean_ws_time_per_instance           FLOAT   COMMENT 'Mean Web Service Time Per Instance for complete period of data fetched',
    std_ws_time_per_instance            FLOAT   COMMENT 'Standard Deviation of Web Service Time Per Instance for Implementation Type over complete period of data fetched',
    mean_tot_time_per_instance          FLOAT   COMMENT 'Mean Total Time Per Instance for complete period of data fetched',
    std_tot_time_per_instance           FLOAT   COMMENT 'Stadard Deviation of Total Time Per Instance for Implementation Type over complete period of data fetched'
)
PARTITIONED BY (
    month                               DATE    COMMENT 'Month Timestamp of the Prime Metrics'
) STORED AS PARQUET;