SELECT implementationType, moduleName, OX20Enabled, migrateableBehavior
FROM implementationTypes
WHERE implementationTypeIsEndOfLifeForVersion = False
    AND composite = False