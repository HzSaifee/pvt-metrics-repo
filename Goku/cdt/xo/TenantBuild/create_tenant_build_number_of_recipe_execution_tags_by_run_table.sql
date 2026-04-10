CREATE TABLE IF NOT EXISTS CDT_TENANT_BUILD_NUMBER_OF_RECIPE_EXECUTION_TAGS_BY_RUN_TABLE_NAME_TO_SET (
    time                                   TIMESTAMP   COMMENT 'Time of the log event',
    recipe_name                            VARCHAR     COMMENT 'Name of the recipe being executed',
    recipe_execution_tags                  VARCHAR     COMMENT 'Tags selected for tenant build',
    recipe_execution_tags_count            BIGINT      COMMENT 'Number of tags selected for tenant build'
)