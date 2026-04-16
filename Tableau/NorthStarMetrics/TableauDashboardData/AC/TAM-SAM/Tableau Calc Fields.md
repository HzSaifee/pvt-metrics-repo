# Tableau Calculated Fields

## Penetration Overall Datasource

### 1. Active Customer Count

**Filters:** None

```
COUNTD(
    IF [is_active_customer] = True
    THEN [sf_account_id]
    END
)
```

### 2. Active Deployment Count - All

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
    THEN [sf_account_id]
    END
)
```

### 3. Active Deployment Count - Initial

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 4. Active Deployment Count - Phase X

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
        AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 5. Activity Count - Initial - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 6. Activity Count - Initial - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 7. Activity Count - Phase X - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 8. Activity Count - Phase X - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 9. Activity Customer Count - All

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
    THEN [sf_account_id]
    END
)
```

### 10. Activity Customer Count - All - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

---

## Penetration Per Tool Datasource

### 1. Active Customer Count

**Filters:** None

```
COUNTD(
    IF [is_active_customer] = True
    THEN [sf_account_id]
    END
)
```

### 2. Active Deployment Count - All

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
    THEN [sf_account_id]
    END
)
```

### 3. Active Deployment Count - Initial

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 4. Active Deployment Count - Phase X

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
COUNTD(
    IF [is_active_deployment] = True
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 5. Activity Count - Initial - CT_TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 6. Activity Count - Initial - CT_TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 7. Activity Count - Initial - FR_MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'FR_MR'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 8. Activity Count - Phase X - CT_TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 9. Activity Count - Phase X - CT_TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 10. Activity Count - Phase X - FR_MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'FR_MR'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 11. Activity Customer Count - CT_TC

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
    THEN [sf_account_id]
    END
)
```

### 12. Activity Customer Count - CT_TC - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_group] = 'CT_TC_AS'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 13. Activity Count - Initial - CT - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 14. Activity Count - Initial - CT - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 15. Activity Count - Initial - TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 16. Activity Count - Initial - TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 17. Activity Count - Initial - AS - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
       AND [deployment_type] = 'Initial Deployment'
    THEN [sf_account_id]
    END
)
```

### 18. Activity Count - Initial - AS - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 19. Activity Count - Initial - FR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Foundation Recipe'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 20. Activity Count - Initial - MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Migration Recipe'
       AND [deployment_type] = 'Initial Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 21. Activity Count - Phase X - CT - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 22. Activity Count - Phase X - CT - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 23. Activity Count - Phase X - TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 24. Activity Count - Phase X - TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 25. Activity Count - Phase X - AS - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
       AND [deployment_type] = 'Subsequent Deployment'
    THEN [sf_account_id]
    END
)
```

### 26. Activity Count - Phase X - AS - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 27. Activity Count - Phase X - FR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Foundation Recipe'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 28. Activity Count - Phase X - MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Migration Recipe'
       AND [deployment_type] = 'Subsequent Deployment'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 29. Activity Customer Count - CT

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
    THEN [sf_account_id]
    END
)
```

### 30. Activity Customer Count - CT - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Change Tracker'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 31. Activity Customer Count - Tenant Compare

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
    THEN [sf_account_id]
    END
)
```

### 32. Activity Customer Count - TC - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Tenant Compare'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```

### 33. Activity Customer Count - AS

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
    THEN [sf_account_id]
    END
)
```

### 34. Activity Customer Count - AS - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
       AND [tool_type] = 'Adhoc Scope'
       AND [customer_status] = 'Migrated'
    THEN [sf_account_id]
    END
)
```
