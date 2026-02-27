SELECT implementationType, moduleName, OX20Enabled
FROM implementationTypes
WHERE implementationTypeIsEndOfLifeForVersion = False
    AND composite = False