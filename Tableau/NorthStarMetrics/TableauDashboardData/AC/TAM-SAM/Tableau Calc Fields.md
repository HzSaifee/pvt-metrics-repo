# Tableau Calculated Fields

## TAM Adoption Trend Datasource

### 1. MoM Change

```tableau
IF FIRST() = 0 THEN NULL
ELSE
  (ZN(SUM([penetration_pct])) - LOOKUP(ZN(SUM([penetration_pct])), -1))
  / ABS(LOOKUP(ZN(SUM([penetration_pct])), -1))
END
```

- **Compute Using:** `snapshot_month`

### 2. MoM Up

```tableau
IF [MoM Change] >= 0 THEN [MoM Change] END
```

- **Compute Using:** `snapshot_month`
- **Format:** Custom Number → `△ 0.0%; ; 0%`

### 3. MoM Down

```tableau
IF [MoM Change] < 0 THEN ABS([MoM Change]) END
```

- **Compute Using:** `snapshot_month`
- **Format:** Custom Number → `▽ -0.0%`

---

## TAM Penetration Counts Datasource

### Penetration % Fields

### 1. Penetration % - Initial - Tool Usage

```
SUM([max_initial_tool_usage]) / SUM([avg_deploy_initial]) * 100
```

### 2. Penetration % - Initial - Migrated

```
SUM([max_initial_migrated]) / SUM([avg_deploy_initial]) * 100
```

### 3. Penetration % - Phase X - Tool Usage

```
SUM([max_phase_x_tool_usage]) / SUM([avg_deploy_phase_x]) * 100
```

### 4. Penetration % - Phase X - Migrated

```
SUM([max_phase_x_migrated]) / SUM([avg_deploy_phase_x]) * 100
```

### 5. Penetration % - Customer - Tool Usage

```
SUM([max_customer_ct_tc]) / SUM([avg_active_customers]) * 100
```

### 6. Penetration % - Customer - Migrated

```
SUM([max_customer_ct_tc_migrated]) / SUM([avg_active_customers]) * 100
```

### 7. Penetration % - Initial - CT_TC - Tool Usage

```
SUM([max_initial_ct_tc_tool_usage]) / SUM([avg_deploy_initial]) * 100
```

### 8. Penetration % - Initial - CT_TC - Migrated

```
SUM([max_initial_ct_tc_migrated]) / SUM([avg_deploy_initial]) * 100
```

### 9. Penetration % - Phase X - CT_TC - Tool Usage

```
SUM([max_phase_x_ct_tc_tool_usage]) / SUM([avg_deploy_phase_x]) * 100
```

### 10. Penetration % - Phase X - CT_TC - Migrated

```
SUM([max_phase_x_ct_tc_migrated]) / SUM([avg_deploy_phase_x]) * 100
```

### 11. Penetration % - Initial - FR_MR - Migrated

```
SUM([max_initial_fr_mr_migrated]) / SUM([avg_deploy_initial]) * 100
```

### 12. Penetration % - Phase X - FR_MR - Migrated

```
SUM([max_phase_x_fr_mr_migrated]) / SUM([avg_deploy_phase_x]) * 100
```

### TAM Absolute Numbers (Static 80%)

### 13. TAM %

```
80
```

### 14. TAM Count - Active Deployment - Initial

```
ROUND(([TAM %] / 100) * SUM([avg_deploy_initial]))
```

### 15. TAM Count - Active Deployment - Phase X

```
ROUND(([TAM %] / 100) * SUM([avg_deploy_phase_x]))
```

### 16. TAM Count - Active Customers

```
ROUND(([TAM %] / 100) * SUM([avg_active_customers]))
```

### 17. Market Filter

```
[market_segment] = [Market]
```

### Formatting Notes

- **All Penetration % fields (1–12):** Format as Custom Number → `0.00"%"`
  (literal `%` suffix, NOT Tableau percentage format — the formula already multiplies by 100)
- **TAM %** field Format as Customer Number → `0"%"`
- **TAM Count fields (13–15):** Format as Number (Integer), no decimal places
- **Denominator fields** (`avg_active_customers`, `avg_deploy_*`): Display as `ROUND(SUM([field]))`, format as Number (Integer)
- `[Market]` is the existing Market parameter (String type).
Place on Filters shelf → select `True`.
This replaces the old 30-line IF/ELSEIF Market Filter that inspected
`enterprise_size_group`, `deployment_partner`, `deployment_phase`, etc.
The SQL already pre-computed segment membership.

---

# Older Calculated Fields of Older Datasource

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

### 2. Active Customer Count - 80%

**Filters:** None

```
[Active Customer Count] * 0.8
```

### 3. Active Deployment Count - All

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

### 4. Active Deployment Count - Initial

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

### 5. Active Deployment Count - Initial - 80%

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
[Active Deployment Count - Initial] * 0.8
```

### 6. Active Deployment Count - Phase X

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

### 7. Active Deployment Count - Phase X - 80%

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
[Active Deployment Count - Phase X] * 0.8
```

### 8. Activity Count - Initial - Tool Usage

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

### 9. Activity Count - Initial - Migrated

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

### 10. Activity Count - Phase X - Tool Usage

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

### 11. Activity Count - Phase X - Migrated

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

### 12. Activity Customer Count - All

**Filters:**

- `User_Type` includes only: Null, Customer

```
COUNTD(
    IF [has_activity] = True
    THEN [sf_account_id]
    END
)
```

### 13. Activity Customer Count - All - Migrated

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

### 14. Penetration % - Customer - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Activity Customer Count - All - Migrated] / [Active Customer Count] * 100
```

### 15. Penetration % - Customer - Tool Usage

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Activity Customer Count - All] / [Active Customer Count] * 100
```

### 16. Penetration % - Initial - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Initial - Migrated] / [Active Deployment Count - Initial] * 100
```

### 17. Penetration % - Initial - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Initial - Tool Usage] / [Active Deployment Count - Initial] * 100
```

### 18. Penetration % - Phase X - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Phase X - Migrated] / [Active Deployment Count - Phase X] * 100
```

### 19. Penetration % - Phase X - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Phase X - Tool Usage] / [Active Deployment Count - Phase X] * 100
```

### 20. TAM Customer

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Active Customer Count - 80%] / [Active Customer Count] * 100
```

### 21. TAM Initial Deployments

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Active Deployment Count - Initial - 80%] / [Active Deployment Count - Initial] * 100
```

### 22. TAM Phase X Deployments

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Active Deployment Count - Phase X - 80%] / [Active Deployment Count - Phase X] * 100
```

### 23. Market Filter

```
IF [Market] = 'All' THEN
    TRUE
ELSEIF [Market] = 'LE' THEN
    [enterprise_size_group] = 'LE'
ELSEIF [Market] = 'ME' THEN
    [enterprise_size_group] = 'ME'
ELSEIF [Market] = 'GO' THEN
    [Is Workday GO Customer?] = TRUE
ELSEIF [Market] = 'GO Partners' THEN
    (
        CONTAINS(UPPER([deployment_partner]), 'ALBIDA') OR
        CONTAINS(UPPER([deployment_partner]), 'APEX') OR
        CONTAINS(UPPER([deployment_partner]), 'TOPBLOC') OR
        CONTAINS(UPPER([deployment_partner]), 'BNB') OR
        CONTAINS(UPPER([deployment_partner]), 'BNET BUILDERS') OR
        CONTAINS(UPPER([deployment_partner]), 'BUSINESS NETWORK BUILDERS') OR
        CONTAINS(UPPER([deployment_partner]), 'HR PATH') OR
        CONTAINS(UPPER([deployment_partner]), 'KAINOS') OR
        CONTAINS(UPPER([deployment_partner]), 'KNOWBRIST') OR
        CONTAINS(UPPER([deployment_partner]), 'THREE LINK') OR
        CONTAINS(UPPER([deployment_partner]), 'MERCER') OR
        CONTAINS(UPPER([deployment_partner]), 'OKORIO') OR
        CONTAINS(UPPER([deployment_partner]), 'OKARIO') OR
        CONTAINS(UPPER([deployment_partner]), 'THREE PLUS') OR
        CONTAINS(UPPER([deployment_partner]), '3PLUS')
    )
ELSEIF [Market] = 'Launch/Express' THEN
    CONTAINS(UPPER([deployment_phase]), 'LAUNCH') OR 
    CONTAINS(UPPER([deployment_phase]), 'EXPRESS')
ELSE
    FALSE
END
```

### Market Parameter Value Mappings

| Parameter Value | Display Text |
| :--- | :--- |
| `All` | All |
| `LE` | LE >3500 |
| `ME` | ME <3500 |
| `GO Partners` | GO Partners |
| `GO Customers` | WD (ZDD) GO |
| `Launch/Express` | Launch/Express |

### 24. Is Workday GO Customer?

```
NOT ISNULL([accountid])
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

### 2. Active Customer Count - 80%

**Filters:** None

```
[Active Customer Count] * 0.8
```

### 3. Active Deployment Count - All

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

### 4. Active Deployment Count - Initial

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

### 5. Active Deployment Count - Initial - 80%

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
[Active Deployment Count - Initial] * 0.8
```

### 6. Active Deployment Count - Phase X

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

### 7. Active Deployment Count - Phase X - 80%

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment

```
[Active Deployment Count - Phase X] * 0.8
```

### 8. Activity Count - Initial - CT_TC - Tool Usage

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

### 9. Activity Count - Initial - CT_TC - Migrated

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

### 10. Activity Count - Initial - FR_MR - Migrated

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

### 11. Activity Count - Phase X - CT_TC - Tool Usage

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

### 12. Activity Count - Phase X - CT_TC - Migrated

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

### 13. Activity Count - Phase X - FR_MR - Migrated

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

### 14. Activity Customer Count - CT_TC

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

### 15. Activity Customer Count - CT_TC - Migrated

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

### 16. Penetration % - Customer - CT_TC - Migrated

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Activity Customer Count - CT_TC - Migrated] / [Active Customer Count] * 100
```

### 17. Penetration % - Customer - CT_TC - Tool Usage

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Activity Customer Count - CT_TC] / [Active Customer Count] * 100
```

### 18. Penetration % - Initial - CT_TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Initial - CT_TC - Migrated] / [Active Deployment Count - Initial] * 100
```

### 19. Penetration % - Initial - CT_TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Initial - CT_TC - Tool Usage] / [Active Deployment Count - Initial] * 100
```

### 20. Penetration % - Initial - FR_MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Initial - FR_MR - Migrated] / [Active Deployment Count - Initial] * 100
```

### 21. Penetration % - Phase X - CT_TC - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Phase X - CT_TC - Migrated] / [Active Deployment Count - Phase X] * 100
```

### 22. Penetration % - Phase X - CT_TC - Tool Usage

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Phase X - CT_TC - Tool Usage] / [Active Deployment Count - Phase X] * 100
```

### 23. Penetration % - Phase X - FR_MR - Migrated

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Activity Count - Phase X - FR_MR - Migrated] / [Active Deployment Count - Phase X] * 100
```

### 24. TAM Customer

**Filters:**

- `User_Type` includes only: Null, Customer

```
[Active Customer Count - 80%] / [Active Customer Count] * 100
```

### 25. TAM Initial Deployments

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Active Deployment Count - Initial - 80%] / [Active Deployment Count - Initial] * 100
```

### 26. TAM Phase X Deployments

**Filters:**

- `Deployment_Phase` excluding: Customer Enablement, Customer Led, No Deployment
- `Deployment_Product` excluding: No Deployment
- `Segment` excluding: US Federal
- `User_Type` includes only: Null, Implementer

```
[Active Deployment Count - Phase X - 80%] / [Active Deployment Count - Phase X] * 100
```

### 27. Market Filter

```
IF [Market] = 'All' THEN
    TRUE
ELSEIF [Market] = 'LE' THEN
    [enterprise_size_group] = 'LE'
ELSEIF [Market] = 'ME' THEN
    [enterprise_size_group] = 'ME'
ELSEIF [Market] = 'GO' THEN
    [Is Workday GO Customer?] = TRUE
ELSEIF [Market] = 'GO Partners' THEN
    (
        CONTAINS(UPPER([deployment_partner]), 'ALBIDA') OR
        CONTAINS(UPPER([deployment_partner]), 'APEX') OR
        CONTAINS(UPPER([deployment_partner]), 'TOPBLOC') OR
        CONTAINS(UPPER([deployment_partner]), 'BNB') OR
        CONTAINS(UPPER([deployment_partner]), 'BNET BUILDERS') OR
        CONTAINS(UPPER([deployment_partner]), 'BUSINESS NETWORK BUILDERS') OR
        CONTAINS(UPPER([deployment_partner]), 'HR PATH') OR
        CONTAINS(UPPER([deployment_partner]), 'KAINOS') OR
        CONTAINS(UPPER([deployment_partner]), 'KNOWBRIST') OR
        CONTAINS(UPPER([deployment_partner]), 'THREE LINK') OR
        CONTAINS(UPPER([deployment_partner]), 'MERCER') OR
        CONTAINS(UPPER([deployment_partner]), 'OKORIO') OR
        CONTAINS(UPPER([deployment_partner]), 'OKARIO') OR
        CONTAINS(UPPER([deployment_partner]), 'THREE PLUS') OR
        CONTAINS(UPPER([deployment_partner]), '3PLUS')
    )
ELSEIF [Market] = 'Launch/Express' THEN
    CONTAINS(UPPER([deployment_phase]), 'LAUNCH') OR 
    CONTAINS(UPPER([deployment_phase]), 'EXPRESS')
ELSE
    FALSE
END
```

### Market Parameter Value Mappings

| Parameter Value | Display Text |
| :--- | :--- |
| `All` | All |
| `LE` | LE >3500 |
| `ME` | ME <3500 |
| `GO Partners` | GO Partners |
| `GO Customers` | WD (ZDD) GO |
| `Launch/Express` | Launch/Express |

### 28. Is Workday GO Customer?

```
NOT ISNULL([accountid])
```

---

# Calculated Fields that were Never Created but had Potential

## Penetration Per Tool Datasource

### 1. Activity Count - Initial - CT - Tool Usage

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

### 2. Activity Count - Initial - CT - Migrated

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

### 3. Activity Count - Initial - TC - Tool Usage

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

### 4. Activity Count - Initial - TC - Migrated

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

### 5. Activity Count - Initial - AS - Tool Usage

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

### 6. Activity Count - Initial - AS - Migrated

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

### 7. Activity Count - Initial - FR - Migrated

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

### 8. Activity Count - Initial - MR - Migrated

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

### 9. Activity Count - Phase X - CT - Tool Usage

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

### 10. Activity Count - Phase X - CT - Migrated

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

### 11. Activity Count - Phase X - TC - Tool Usage

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

### 12. Activity Count - Phase X - TC - Migrated

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

### 13. Activity Count - Phase X - AS - Tool Usage

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

### 14. Activity Count - Phase X - AS - Migrated

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

### 15. Activity Count - Phase X - FR - Migrated

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

### 16. Activity Count - Phase X - MR - Migrated

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

### 17. Activity Customer Count - CT

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

### 18. Activity Customer Count - CT - Migrated

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

### 19. Activity Customer Count - Tenant Compare

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

### 20. Activity Customer Count - TC - Migrated

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

### 21. Activity Customer Count - AS

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

### 22. Activity Customer Count - AS - Migrated

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
