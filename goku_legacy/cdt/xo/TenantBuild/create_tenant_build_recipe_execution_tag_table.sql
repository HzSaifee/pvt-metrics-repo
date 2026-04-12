CREATE TABLE IF NOT EXISTS CDT_TENANT_BUILD_RECIPE_EXECUTION_TAG_TABLE_NAME_TO_SET (
    recipe_name                         VARCHAR    COMMENT 'Name of the recipe being executed',
    recipe_execution_tag                VARCHAR    COMMENT 'Tag selected for tenant build',
    recipe_execution_tag_count          BIGINT    COMMENT 'Tag count for tenant build'
)