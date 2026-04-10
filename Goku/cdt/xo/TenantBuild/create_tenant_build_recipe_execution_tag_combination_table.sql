CREATE TABLE IF NOT EXISTS CDT_TENANT_BUILD_RECIPE_EXECUTION_TAG_COMBINATION_TABLE_NAME_TO_SET (
    recipe_name                            VARCHAR    COMMENT 'Name of the recipe being executed',
    recipe_execution_tag_combination       VARCHAR    COMMENT 'Tag combination selected for tenant build',
    recipe_execution_tag_combination_count BIGINT    COMMENT 'Tag combination count for tenant build'
)