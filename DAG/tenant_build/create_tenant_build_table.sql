CREATE TABLE IF NOT EXISTS {{ table_name }} (
    time                                TIMESTAMP   COMMENT 'Time of the log event',
    elapsed_time                        BIGINT     COMMENT 'Total time elapsed for the build in seconds',
    migration_success_rate              BIGINT     COMMENT 'Success rate of migrated instances (multiplied by 100)',
    migrated_error_count                BIGINT     COMMENT 'Total number of instances failed to migrate',
    total_instance_count                BIGINT     COMMENT 'Total instance count for the build',
    recipe_name                         VARCHAR    COMMENT 'Name of the recipe being executed',
    build_type                          VARCHAR    COMMENT 'Type of tenant build',
    build_status                        VARCHAR    COMMENT 'Build status',
    wd_env                              VARCHAR    COMMENT 'Environment (PROD, IMPL, etc.)',
    customer_tenant                     VARCHAR    COMMENT 'Customer tenant name'
)
