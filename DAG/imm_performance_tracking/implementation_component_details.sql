SELECT implementationType, implementationComponentsForImplementationType, migrateableBehavior
FROM implementationTypes
WHERE implementationTypeIsEndOfLifeForVersion = False
    AND composite = False