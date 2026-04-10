CREATE TABLE IF NOT EXISTS {{ table_name }} (
    recipe_name                            VARCHAR    COMMENT 'Name of the recipe being executed',
    recipe_execution_tag_combination       VARCHAR    COMMENT 'Tag combination selected for tenant build',
    recipe_execution_tag_combination_count BIGINT    COMMENT 'Tag combination count for tenant build'
)
