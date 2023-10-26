SELECT specificTypes, relatedNon_PrimaryTypes
FROM implementationTypes
WHERE implementationTypeIsEndOfLifeForVersion=False
    AND composite=True