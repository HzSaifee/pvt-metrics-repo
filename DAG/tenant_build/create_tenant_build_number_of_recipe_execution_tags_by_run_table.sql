CREATE TABLE IF NOT EXISTS {{ table_name }} (
    time                                   TIMESTAMP   COMMENT 'Time of the log event',
    recipe_name                            VARCHAR     COMMENT 'Name of the recipe being executed',
    recipe_execution_tags                  VARCHAR     COMMENT 'Tags selected for tenant build',
    recipe_execution_tags_count            BIGINT      COMMENT 'Number of tags selected for tenant build'
)
