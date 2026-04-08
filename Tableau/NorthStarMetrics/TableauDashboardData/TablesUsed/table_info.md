# Database Tables Reference

> **Last Updated:** February 23, 2026  
> **Purpose:** Schema documentation for Tableau Dashboard data sources

---

## Table of Contents

- [Trino (dw)](#trino-dw)
  - [lookup_db](#schema-lookup_db)
  - [swh](#schema-swh)
- [Amazon Redshift (edwprod)](#amazon-redshift-edwprod)
  - [ccr_data_hub](#schema-ccr_data_hub)

---

# Trino (dw)

## Schema: lookup_db

### TABLE: sfdc_account_details

| Column | Type |
|--------|------|
| sf_account_id | VARCHAR |
| billing_id | VARCHAR |
| account_status | VARCHAR |
| account_type | VARCHAR |
| account_name | VARCHAR |
| number_of_employees | INTEGER |
| segment_size_l1 | VARCHAR |
| segment_size_l2 | VARCHAR |
| segment_size_l3 | VARCHAR |
| enterprise_size | VARCHAR |
| enterprise_size_group | VARCHAR |
| segment | VARCHAR |
| country | VARCHAR |
| sgi_ochro_region | VARCHAR |
| sgi_ocfo_industry | VARCHAR |
| super_industry | VARCHAR |
| industry | VARCHAR |
| industry_level | VARCHAR |
| account_classification | VARCHAR |
| intitial_deployment_methodology | VARCHAR |
| allowed_to_name | VARCHAR |
| customer_of | VARCHAR |
| initial_deployment_partner_account_id | VARCHAR |
| initial_deployment_partner_nickname | VARCHAR |
| initial_deployment_approach | VARCHAR |
| assumed_enterprise_start_date | DATE |
| assumed_enterprise_go_live_date | DATE |
| assumed_enterprise_end_date | DATE |
| merged_number | DOUBLE |
| customer_number | DOUBLE |
| partner_number | DOUBLE |

---

### TABLE: sfdc_customer_account_tenants

| Column | Type |
|--------|------|
| sf_account_id | VARCHAR |
| billing_id | VARCHAR |
| tenant_prefix | VARCHAR |
| tenant_name | VARCHAR |
| tenant_type | VARCHAR |
| confidence_level | VARCHAR |
| environment_name | VARCHAR |
| tenant_subtype | VARCHAR |
| tenant_pattern | VARCHAR |

---

### TABLE: sfdc_deployments

| Column | Type |
|--------|------|
| customer_sf_account_id | VARCHAR |
| sf_deployment_id | VARCHAR |
| name | VARCHAR |
| phase | VARCHAR |
| stage | VARCHAR |
| type | VARCHAR |
| priming_category | VARCHAR |
| priming_partner_sf_account_id | VARCHAR |
| priming_partner_name | VARCHAR |
| overall_health | VARCHAR |
| overall_health_score | DOUBLE |
| overall_status | VARCHAR |
| service_type | VARCHAR |
| psa_project_name | VARCHAR |
| primary_language | VARCHAR |
| languages | VARCHAR |
| deployment_start_date | DATE |
| deployment_completion_date | DATE |
| product_area | VARCHAR |
| product_function | VARCHAR |
| function_production_move_date_target | DATE |
| function_production_move_date_actual | DATE |

---

### TABLE: sfdc_customer_tenants

| Column | Type |
|--------|------|
| account_name | VARCHAR |
| customer_number | DOUBLE |
| data_center | VARCHAR |
| tenant_name | VARCHAR |
| tenant_type | VARCHAR |
| tenant_sub_type | VARCHAR |
| status | VARCHAR |
| workday_version | VARCHAR |
| tenant_created_date | TIMESTAMP |
| tenant_start_date | TIMESTAMP |
| tenant_expire_date | TIMESTAMP |
| jumbo_tenant | BOOLEAN |
| grid_flag | BOOLEAN |
| account_status | VARCHAR |
| billing_id | VARCHAR |
| sf_id | VARCHAR |
| acquisition_external_id_c | VARCHAR |

---

### TABLE: sfdc_account_tenant_map

| Column | Type |
|--------|------|
| account_type | VARCHAR |
| sf_account_id | VARCHAR |
| billing_id | VARCHAR |
| account_prefix | VARCHAR |
| env_type | VARCHAR |
| tenant_type | VARCHAR |
| confidence_level | VARCHAR |
| environment_name | VARCHAR |
| tenant_name | VARCHAR |
| source_data | VARCHAR |

---

## Schema: swh

### TABLE: tenant_compare_event_log

| Column | Type |
|--------|------|
| billing_id | VARCHAR |
| cc_tenant | VARCHAR |
| client_id | VARCHAR |
| common_request_id | VARCHAR |
| duration | BIGINT |
| event_result | VARCHAR |
| event_type | VARCHAR |
| failure_reason | VARCHAR |
| filter_selection_category | VARCHAR |
| job_definition | VARCHAR |
| job_id | VARCHAR |
| job_runtime_wid | VARCHAR |
| one_click_diff | BOOLEAN |
| source_tenant_env | VARCHAR |
| source_tenant | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_offset | BIGINT |
| swh_kafka_partition | BIGINT |
| swh_version_build | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_year | VARCHAR |
| target_tenant_env | VARCHAR |
| target_tenant | VARCHAR |
| tenant_compare_wid | VARCHAR |
| tenant | VARCHAR |
| time_end | TIMESTAMP |
| time_start | TIMESTAMP |
| time | TIMESTAMP |
| user_type | VARCHAR |
| user_wid | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env_status | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| source_id | VARCHAR |
| excludedattributes | VARCHAR |
| tenant_compare_scope_wid | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: change_tracker_event_log

| Column | Type |
|--------|------|
| change_tracker_wid | VARCHAR |
| common_request_id | VARCHAR |
| count_categories_selected | BIGINT |
| count_changed_instances_by_ui | BIGINT |
| count_changed_instances_by_ws | BIGINT |
| count_changed_instances_unclassified | BIGINT |
| count_changed_instances | BIGINT |
| count_data_load_records | BIGINT |
| count_processed_transactions | BIGINT |
| count_skipped_processed_transactions | BIGINT |
| count_workday_accounts_selected | BIGINT |
| customer_billing_id | VARCHAR |
| duration | BIGINT |
| event_result | VARCHAR |
| failure_reason | VARCHAR |
| filter_selection_category | VARCHAR |
| from_moment | TIMESTAMP |
| include_web_services | BOOLEAN |
| job_runtime_wid | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_offset | BIGINT |
| swh_kafka_partition | BIGINT |
| swh_version_build | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_year | VARCHAR |
| tenant_env | VARCHAR |
| tenant | VARCHAR |
| time_end | TIMESTAMP |
| time_start | TIMESTAMP |
| time | TIMESTAMP |
| to_moment | TIMESTAMP |
| user_self_selection | BOOLEAN |
| user_type | VARCHAR |
| user_wid | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env_status | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| count_deleted_instances | BIGINT |
| count_dnu_instances | BIGINT |
| restricted_user | BOOLEAN |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: scopes_input_type_metrics

| Column | Type |
|--------|------|
| wd_env | VARCHAR |
| swh_version_year | VARCHAR |
| scope_id | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_server | VARCHAR |
| input_type | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_service_instance | VARCHAR |
| swh_kafka_offset | BIGINT |
| wd_env_status | VARCHAR |
| swh_version_build | VARCHAR |
| wd_env_physical | VARCHAR |
| time | TIMESTAMP |
| input_type_count | BIGINT |
| swh_kafka_partition | BIGINT |
| swh_version_week | VARCHAR |
| wd_dc_physical | VARCHAR |
| common_request_id | VARCHAR |
| input_id | VARCHAR |
| scope_external_id | VARCHAR |
| user_type | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: scopes_metrics

| Column | Type |
|--------|------|
| tenant_name | VARCHAR |
| wd_env | VARCHAR |
| swh_version_year | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_server | VARCHAR |
| scope_selection_type | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| wd_dc_type | VARCHAR |
| common_request_id | VARCHAR |
| wd_service_instance | VARCHAR |
| swh_kafka_offset | BIGINT |
| wd_env_status | VARCHAR |
| swh_version_build | VARCHAR |
| scope_id | VARCHAR |
| wd_env_physical | VARCHAR |
| time | TIMESTAMP |
| swh_kafka_partition | BIGINT |
| swh_version_week | VARCHAR |
| wd_dc_physical | VARCHAR |
| job_name | VARCHAR |
| scope_external_id | VARCHAR |
| additive | BOOLEAN |
| user_type | VARCHAR |
| restricted_user | BOOLEAN |
| filtered_input_type | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: migration_event_log

| Column | Type |
|--------|------|
| cc_billing_id | VARCHAR |
| cc_tenant | VARCHAR |
| common_request_id | VARCHAR |
| duration | BIGINT |
| effective_date_strategy | VARCHAR |
| effective_date | VARCHAR |
| event_id | VARCHAR |
| event_result | VARCHAR |
| event_type | VARCHAR |
| filter_migration | BOOLEAN |
| migration_id | VARCHAR |
| preceding_event_id | VARCHAR |
| source_content_extraction_id | VARCHAR |
| source_object_subtype | VARCHAR |
| source_object_type | VARCHAR |
| source_object_wid | VARCHAR |
| source_tenant_env | VARCHAR |
| source_tenant | VARCHAR |
| stage | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_offset | BIGINT |
| swh_kafka_partition | BIGINT |
| swh_version_build | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_year | VARCHAR |
| target_object_wid | VARCHAR |
| target_tenant_env | VARCHAR |
| target_tenant | VARCHAR |
| time_end | TIMESTAMP |
| time_start | TIMESTAMP |
| time | TIMESTAMP |
| translations_enabled | BOOLEAN |
| type_of_migration | VARCHAR |
| user_type | VARCHAR |
| user_wid | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env_status | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| web_service_version | VARCHAR |
| app_id | VARCHAR |
| app_version_id | VARCHAR |
| org_id | VARCHAR |
| extract_version | BIGINT |
| source_object_id | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| initiated_by | VARCHAR |
| bundle_id | VARCHAR |
| bundle_version | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: tenant_build

| Column | Type |
|--------|------|
| time | TIMESTAMP |
| customer | VARCHAR |
| integration | VARCHAR |
| commonrequestid | VARCHAR |
| build_type | VARCHAR |
| recipe_name | VARCHAR |
| error_limit | VARCHAR |
| source_tenant | VARCHAR |
| target_tenant | VARCHAR |
| build_status | VARCHAR |
| percent_complete | BIGINT |
| migration_success_rate | BIGINT |
| elapsed_time | BIGINT |
| recipe_preview_status | VARCHAR |
| recipe_execution_tags | VARCHAR |
| total_instance_count | BIGINT |
| migrated_error_count | BIGINT |
| number_of_steps | BIGINT |
| customer_tenant | VARCHAR |
| total_record_count | BIGINT |
| migrated_record_count | BIGINT |
| customer_billing_id | VARCHAR |
| swh_version_year | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_build | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_partition | BIGINT |
| swh_kafka_offset | BIGINT |
| wd_env_logical | VARCHAR |
| wd_env_status | VARCHAR |
| migration_recipe_id | VARCHAR |
| migration_recipe_wid | VARCHAR |
| migration_recipe_execution_id | VARCHAR |
| recipe_configuration_id | VARCHAR |
| recipe_configuration_wid | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: loadapirequests

| Column | Type |
|--------|------|
| time | TIMESTAMP |
| customer | VARCHAR |
| integration | VARCHAR |
| commonrequestid | VARCHAR |
| originator | VARCHAR |
| workday_version | VARCHAR |
| implementation_component | VARCHAR |
| validate_only | VARCHAR |
| enable_bulk | VARCHAR |
| max_errors | BIGINT |
| response_format | VARCHAR |
| payload_size | BIGINT |
| correlation_id | VARCHAR |
| http_status_code | BIGINT |
| elapsed_time | BIGINT |
| bulk_lite_enabled | BOOLEAN |
| swh_version_year | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_build | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_partition | BIGINT |
| swh_kafka_offset | BIGINT |
| implementation_type | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_env_status | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| api_version | VARCHAR |
| authentication_type | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

### TABLE: dataload_metrics

| Column | Type |
|--------|------|
| time | TIMESTAMP |
| customer | VARCHAR |
| integration | VARCHAR |
| commonrequestid | VARCHAR |
| event_id | VARCHAR |
| sent_date | VARCHAR |
| complete_date | VARCHAR |
| execution_time | BIGINT |
| is_bulk_disabled | BOOLEAN |
| is_bulk_lite | BOOLEAN |
| is_validate | BOOLEAN |
| total_records | BIGINT |
| total_updated_records | BIGINT |
| total_failed_records | BIGINT |
| implementation_component | VARCHAR |
| implementation_component_wid | VARCHAR |
| implementation_type | VARCHAR |
| implementation_type_wid | VARCHAR |
| client_id | VARCHAR |
| customer_tenant | VARCHAR |
| ws_version | VARCHAR |
| status | VARCHAR |
| swh_version_year | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_build | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_partition | BIGINT |
| swh_kafka_offset | BIGINT |
| wd_env_logical | VARCHAR |
| wd_env_status | VARCHAR |
| concurrent_job_wid | VARCHAR |
| common_request_id | VARCHAR |
| is_bulk_enabled | BOOLEAN |
| wd_dc_type | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |
| processed_at_time | BIGINT |
| schema_hashcode | BIGINT |

---

## Schema: cdt

### TABLE: implementation_type_mapping

| Column | Type |
|--------|------|
| entity_name | VARCHAR |
| category | VARCHAR |

---

### TABLE: migrations_blended

| Column | Type |
|--------|------|
| migration_id | VARCHAR |
| type_of_migration | VARCHAR |
| cc_billing_id | VARCHAR |
| customer_name | VARCHAR |
| cc_tenant | VARCHAR |
| effective_date_strategy | VARCHAR |
| effective_date | DATE |
| filter_migration | BOOLEAN |
| translations_enabled | BOOLEAN |
| preceding_migration_id | VARCHAR |
| source_object_type | VARCHAR |
| source_object_subtype | VARCHAR |
| source_object_id | VARCHAR |
| source_tenant_env | VARCHAR |
| source_tenant | VARCHAR |
| target_object_wid | VARCHAR |
| target_tenant_env | VARCHAR |
| target_tenant | VARCHAR |
| event_id | VARCHAR |
| stage | VARCHAR |
| event_type | VARCHAR |
| user_type | VARCHAR |
| user_wid | VARCHAR |
| event_result | VARCHAR |
| time_start | TIMESTAMP |
| time_end | TIMESTAMP |
| duration | INTEGER |
| web_service_version | VARCHAR |
| source_content_extraction_id | VARCHAR |
| source_content_extraction_date | DATE |
| source_content_extraction_env | VARCHAR |
| source_content_extraction_tenant | VARCHAR |
| batch_common_request_id | VARCHAR |
| metric_type | VARCHAR |
| metric_tenant | VARCHAR |
| error_type | VARCHAR |
| impl_type_name | VARCHAR |
| impl_type_wid | VARCHAR |
| impl_type_top_level_flag | BOOLEAN |
| parent_top_level_impl_type_name | VARCHAR |
| parent_top_level_impl_type_wid | VARCHAR |
| count_total_instances | INTEGER |
| count_new_instances | INTEGER |
| count_changed_instances | INTEGER |
| count_no_change_instances | INTEGER |
| count_prereq_instances | INTEGER |
| count_missing_prereq_instances | INTEGER |
| count_push_success | INTEGER |
| count_push_partial | INTEGER |
| count_push_not_attempted | INTEGER |
| count_push_not_changed | INTEGER |
| count_push_ignored | INTEGER |
| count_push_error | INTEGER |
| count_mapping_applied | INTEGER |
| count_mapping_removed | INTEGER |
| count_ed_instances | INTEGER |
| count_extraction_success | INTEGER |
| count_change_with_target_error_instances | INTEGER |
| count_prereq_instances_errors | INTEGER |
| count_extraction_error | INTEGER |
| count_translated_instances | INTEGER |
| count_unique_languages | INTEGER |
| languages | ARRAY |
| metrics_schema_version | VARCHAR |
| wd_dc_physical | VARCHAR |
| count_excluded_instances | INTEGER |
| count_excluded_by_parent_instances | INTEGER |
| app_id | VARCHAR |
| app_version_id | VARCHAR |
| org_id | VARCHAR |
| extract_version | INTEGER |
| initiated_by | VARCHAR |
| count_merge | INTEGER |
| count_override | INTEGER |
| count_missing | INTEGER |
| effective_date_template | DATE |
| effective_date_count | INTEGER |
| missing_dependency_external_id_type | VARCHAR |
| missing_dependency_parent_id_type | VARCHAR |
| wd_event_date | DATE |
| wd_env_type | VARCHAR |

---

### TABLE: dataload_metrics_deployment_data

| Column | Type |
|--------|------|
| time | VARCHAR |
| customer | VARCHAR |
| integration | VARCHAR |
| commonrequestid | VARCHAR |
| event_id | VARCHAR |
| sent_date | VARCHAR |
| complete_date | VARCHAR |
| execution_time | VARCHAR |
| is_bulk_disabled | VARCHAR |
| is_bulk_lite | VARCHAR |
| is_validate | VARCHAR |
| total_records | VARCHAR |
| total_updated_records | VARCHAR |
| total_failed_records | VARCHAR |
| implementation_component | VARCHAR |
| implementation_component_wid | VARCHAR |
| implementation_type | VARCHAR |
| implementation_type_wid | VARCHAR |
| client_id | VARCHAR |
| customer_tenant | VARCHAR |
| ws_version | VARCHAR |
| status | VARCHAR |
| swh_version_year | VARCHAR |
| swh_version_week | VARCHAR |
| swh_version_build | VARCHAR |
| swh_kafka_cluster | VARCHAR |
| swh_kafka_partition | VARCHAR |
| swh_kafka_offset | VARCHAR |
| wd_env_logical | VARCHAR |
| wd_env_status | VARCHAR |
| concurrent_job_wid | VARCHAR |
| common_request_id | VARCHAR |
| is_bulk_enabled | VARCHAR |
| wd_dc_type | VARCHAR |
| wd_dc_physical | VARCHAR |
| wd_env_physical | VARCHAR |
| wd_env | VARCHAR |
| wd_server | VARCHAR |
| wd_service_instance | VARCHAR |
| cluster | VARCHAR |
| source | VARCHAR |
| wd_datatype | VARCHAR |
| wd_dc_provider | VARCHAR |
| wd_env_id | VARCHAR |
| wd_logical | VARCHAR |
| wd_logical_type | VARCHAR |
| wd_objectname | VARCHAR |
| wd_owner | VARCHAR |
| wd_platform | VARCHAR |
| wd_server_role | VARCHAR |
| wd_service | VARCHAR |
| processed_at_time | VARCHAR |
| schema_hashcode | VARCHAR |
| customer_tenant_prefix | VARCHAR |
| account_name | VARCHAR |
| initial_deployment_partner | VARCHAR |
| deployment_name | VARCHAR |
| deployment_type | VARCHAR |
| wd_event_date | VARCHAR |
| wd_env_type | VARCHAR |

---

# Amazon Redshift (edwprod)

## Schema: ccr_data_hub

### TABLE: bv_customer_entitled_sku

| Column | Type |
|--------|------|
| month_key | BIGINT |
| report_snapshot_month | DATE |
| account_id | VARCHAR |
| account_name | VARCHAR |
| customer_industry | VARCHAR |
| customer_tenant_prefix | VARCHAR |
| renewal_date | DATE |
| sku_code | VARCHAR |
| sku_name | VARCHAR |
| purchase_date | DATE |
| earliest_sku_start_date | DATE |
| earliest_go_live | DATE |
| earliest_target_go_live | DATE |
| deployment_status | VARCHAR |
| segment | VARCHAR |
| region | VARCHAR |
| sub_region | VARCHAR |
| initial_go_live_date | DATE |
| account_classification | VARCHAR |
| customer_success_manager | VARCHAR |
| number_of_employees | BIGINT |
| super_industry | VARCHAR |
| account_sku_arr | DOUBLE |
| adoption_sni_flg | BOOLEAN |
| deployment_sni_flg | BOOLEAN |
| deployment_sni_cnt | BIGINT |
| adoption_sni_cnt | BIGINT |
| fiscal_quarter_year_name_fy | VARCHAR |
| external_sku_code | VARCHAR |
| services_disposition | VARCHAR |
| product_normalized | VARCHAR |

---

### TABLE: bv_deployments

| Column | Type |
|--------|------|
| deployments_key | BIGINT |
| deployments_id | VARCHAR |
| deleted_flag | BOOLEAN |
| deployments_name | VARCHAR |
| currency_iso_cd | VARCHAR |
| system_mod_stamp | TIMESTAMP |
| last_activity_date | DATE |
| annual_tax_filing_provider | VARCHAR |
| central_desktop_umbrella_jira | VARCHAR |
| contract_type | VARCHAR |
| contract | VARCHAR |
| customer_number | VARCHAR |
| customer | VARCHAR |
| deployment_completion_date | DATE |
| deployment_description | VARCHAR |
| deployment_name | VARCHAR |
| deployment_phase | VARCHAR |
| deployment_stage | VARCHAR |
| deployment_start_date | DATE |
| deployment_summary | VARCHAR |
| deployment_type | VARCHAR |
| employee_review_type | VARCHAR |
| employee_reviews_start_date | DATE |
| external_transition_date | DATE |
| external_transition_to_production_services_flag | BOOLEAN |
| first_tenant_delivery_date | DATE |
| first_tenant_delivery_notification_flag | BOOLEAN |
| focal_review_end_date | DATE |
| focal_review_start_date | DATE |
| global_rollout_flag | BOOLEAN |
| golden_tenant_jira | VARCHAR |
| implementation_partner | VARCHAR |
| production_flag | BOOLEAN |
| integrations_umbrella_jira | VARCHAR |
| mulitlingual_flag | BOOLEAN |
| open_enrollment_end_date | DATE |
| open_enrollment_start_date | DATE |
| overall_health | VARCHAR |
| overall_status | VARCHAR |
| pay_period_schedule | VARCHAR |
| preferred_tenant_name | VARCHAR |
| primary_language | VARCHAR |
| priming_partner | VARCHAR |
| priming | VARCHAR |
| rapid_deployment_customer_flag | BOOLEAN |
| referenceable_flag | BOOLEAN |
| related_opportunity | VARCHAR |
| scope_of_work_completed_flag | BOOLEAN |
| summary | VARCHAR |
| tenant_requests_umbrella_jira | VARCHAR |
| transition_from_sales_to_services_date | DATE |
| transition_to_customer_success_date | DATE |
| transitioned_from_sales_to_services_flag | BOOLEAN |
| transition_from_customer_to_success_flag | BOOLEAN |
| umbrella_pmo_jira | VARCHAR |
| on_budget_flag | BOOLEAN |
| on_time_flag | BOOLEAN |
| delvery_assurance_operatoins_update | VARCHAR |
| service_type | VARCHAR |
| management_update | VARCHAR |
| case | VARCHAR |
| prime_partner_status | VARCHAR |
| deployment_health_functional_owner | VARCHAR |
| deployment_health_next_steps | VARCHAR |
| deployment_health_quarter_back | VARCHAR |
| last_modified_by_user_account | VARCHAR |
| partner_project_manager | VARCHAR |
| workday_engagement_manager | VARCHAR |
| customer_name | VARCHAR |
| pm_notification_email_sent_flag | BOOLEAN |
| account_type | VARCHAR |
| my_deployment | VARCHAR |
| account_status | VARCHAR |
| customer_id | VARCHAR |
| customer_status | VARCHAR |
| customer_type | VARCHAR |
| customer_led_deployment_flag | BOOLEAN |
| transition_jira | VARCHAR |
| view_summary | VARCHAR |
| current_deployment_update_chage_date | DATE |
| current_deployment_update_changed_by | VARCHAR |
| deployment_stage_change_date | DATE |
| off_shore_resources_acceptable_flag | BOOLEAN |
| ps_region | VARCHAR |
| ignore_checkpoints_flag | BOOLEAN |
| master_build_review_date | DATE |
| master_integration_build_review_date | DATE |
| master_integration_design_review_date | DATE |
| production_move_earliest_date | DATE |
| product_area | VARCHAR |
| requires_da | VARCHAR |
| delivery_assurance_manager | VARCHAR |
| delvery_assurance_integration_manager | VARCHAR |
| platform_deployment | VARCHAR |
| dam_user_id | VARCHAR |
| em_user_ids | VARCHAR |
| my_dam_deployments_flag | BOOLEAN |
| my_em_deployments_flag | BOOLEAN |
| journal_and_invoice_not_required_flag | BOOLEAN |
| deployment_partner_name | VARCHAR |
| bpo2 | VARCHAR |
| bpo3 | VARCHAR |
| bpo | VARCHAR |
| dam_deployment | VARCHAR |
| deployment_inactive_date | DATE |
| first_go_live_deployment_flag | BOOLEAN |
| deployment_locked_flag | BOOLEAN |
| languages | VARCHAR |
| operational2 | VARCHAR |
| operational3 | VARCHAR |
| operational | VARCHAR |
| psa_project_name | VARCHAR |
| release | VARCHAR |
| roadmap | VARCHAR |
| fin_initial_flag | BOOLEAN |
| gcm_initial_flag | BOOLEAN |
| stu_initial_flag | BOOLEAN |
| talent_initial_flag | BOOLEAN |
| executive_watch_list_flag | BOOLEAN |
| hold_back_flag | BOOLEAN |
| dac_region | VARCHAR |
| dam_light_flag | BOOLEAN |
| talent_first_flag | BOOLEAN |
| customer_name_text | VARCHAR |
| customer_success_manager_name | VARCHAR |
| actual_date_filled_deployment_active_flag | BOOLEAN |
| no_current_update_in_the_last_15_days_flag | BOOLEAN |
| no_em_pm_flag | BOOLEAN |
| no_priming_partner_flag | BOOLEAN |
| no_start_date_from_plan_stage_flag | BOOLEAN |
| past_date_target_null_and_no_prod_move_flag | BOOLEAN |
| production_functions_no_countries_at_all_flag | BOOLEAN |
| product_functions_without_countries | VARCHAR |
| wo_production_fuction_from_planning_stage_flag | BOOLEAN |
| ps_subregion | VARCHAR |
| first_move_to_production_date_actual | DATE |
| first_move_to_production_date | DATE |
| first_move_to_production_date_oemb | DATE |
| steering_commitee_last_date | DATE |
| trending | VARCHAR |
| all_pms | VARCHAR |
| all_primary_ems | VARCHAR |
| all_secondary_ems | VARCHAR |
| customer_employees | BIGINT |
| days_to_live_deployment | BIGINT |
| deployed_indicator | BIGINT |
| deployment_count | BIGINT |
| financial_journal_entries | BIGINT |
| invoice_header_information | BIGINT |
| nbr_of_days_after_current_update | BIGINT |
| overall_health_score | DOUBLE |
| partner_primed | BIGINT |
| products_past_date_target | BIGINT |
| projects_with_active_deployment | BIGINT |
| projects_with_no_countries | BIGINT |
| projects_with_no_countries_at_all | BIGINT |
| projects_with_no_current_activities | BIGINT |
| projects_with_no_pms | BIGINT |
| projects_with_no_priming_partner | BIGINT |
| projects_with_no_product_functions | BIGINT |
| projects_with_no_start_dates | BIGINT |
| time_to_value | BIGINT |
| time_to_value_from_prod_max | BIGINT |
| wd_primed | BIGINT |
| advanced_da_flag | BOOLEAN |
| effective_start_date | DATE |
| effective_end_date | DATE |
| current_flag | VARCHAR |
| etl_created_date | TIMESTAMP |
| etl_updated_date | TIMESTAMP |