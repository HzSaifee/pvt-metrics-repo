SELECT implementationType, moduleName
FROM implementationTypes
WHERE implementationTypeIsEndOfLifeForVersion = False
    AND composite = False