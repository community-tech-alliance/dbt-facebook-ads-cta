{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_#PARTNER#_facebook_marketing_cta"
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('ad_account_ab1') }}
select
    cast(id as {{ dbt_utils.type_string() }}) as id,
    cast(age as {{ dbt_utils.type_float() }}) as age,
    cast(name as {{ dbt_utils.type_string() }}) as name,
    cast(owner as {{ dbt_utils.type_float() }}) as owner,
    cast(tax_id as {{ dbt_utils.type_float() }}) as tax_id,
    cast(balance as {{ dbt_utils.type_string() }}) as balance,
    cast(partner as {{ dbt_utils.type_float() }}) as partner,
    cast(rf_spec as {{ type_json() }}) as rf_spec,
    cast(business as {{ type_json() }}) as business,
    cast(currency as {{ dbt_utils.type_string() }}) as currency,
    cast(fb_entity as {{ dbt_utils.type_float() }}) as fb_entity,
    cast(io_number as {{ dbt_utils.type_float() }}) as io_number,
    cast(spend_cap as {{ dbt_utils.type_string() }}) as spend_cap,
    cast(account_id as {{ dbt_utils.type_string() }}) as account_id,
    user_tasks,
    cast(is_personal as {{ dbt_utils.type_float() }}) as is_personal,
    cast(tax_id_type as {{ dbt_utils.type_string() }}) as tax_id_type,
    cast(timezone_id as {{ dbt_utils.type_float() }}) as timezone_id,
    cast(amount_spent as {{ dbt_utils.type_string() }}) as amount_spent,
    cast(business_zip as {{ dbt_utils.type_string() }}) as business_zip,
    capabilities,
    cast({{ empty_string_to_null('created_time') }} as {{ type_timestamp_with_timezone() }}) as created_time,
    cast(line_numbers as {{ dbt_utils.type_float() }}) as line_numbers,
    cast(media_agency as {{ dbt_utils.type_float() }}) as media_agency,
    cast(tos_accepted as {{ type_json() }}) as tos_accepted,
    cast(business_city as {{ dbt_utils.type_string() }}) as business_city,
    cast(business_name as {{ dbt_utils.type_string() }}) as business_name,
    cast(tax_id_status as {{ dbt_utils.type_float() }}) as tax_id_status,
    cast(timezone_name as {{ dbt_utils.type_string() }}) as timezone_name,
    cast(account_status as {{ dbt_utils.type_bigint() }}) as account_status,
    cast(business_state as {{ dbt_utils.type_string() }}) as business_state,
    cast(disable_reason as {{ dbt_utils.type_float() }}) as disable_reason,
    cast(end_advertiser as {{ dbt_utils.type_float() }}) as end_advertiser,
    cast(funding_source as {{ dbt_utils.type_float() }}) as funding_source,
    cast(business_street as {{ dbt_utils.type_string() }}) as business_street,
    cast(business_street2 as {{ dbt_utils.type_string() }}) as business_street2,
    cast(min_daily_budget as {{ dbt_utils.type_float() }}) as min_daily_budget,
    {{ cast_to_boolean('is_prepay_account') }} as is_prepay_account,
    cast(user_tos_accepted as {{ type_json() }}) as user_tos_accepted,
    {{ cast_to_boolean('is_tax_id_required') }} as is_tax_id_required,
    cast(end_advertiser_name as {{ dbt_utils.type_string() }}) as end_advertiser_name,
    cast(business_country_code as {{ dbt_utils.type_string() }}) as business_country_code,
    cast(failed_delivery_checks as {{ dbt_utils.type_float() }}) as failed_delivery_checks,
    cast(funding_source_details as {{ type_json() }}) as funding_source_details,
    {{ cast_to_boolean('is_direct_deals_enabled') }} as is_direct_deals_enabled,
    {{ cast_to_boolean('has_migrated_permissions') }} as has_migrated_permissions,
    {{ cast_to_boolean('is_notifications_enabled') }} as is_notifications_enabled,
    cast(timezone_offset_hours_utc as {{ dbt_utils.type_float() }}) as timezone_offset_hours_utc,
    {{ cast_to_boolean('can_create_brand_lift_study') }} as can_create_brand_lift_study,
    {{ cast_to_boolean('offsite_pixels_tos_accepted') }} as offsite_pixels_tos_accepted,
    {{ cast_to_boolean('has_advertiser_opted_in_odax') }} as has_advertiser_opted_in_odax,
    cast(min_campaign_group_spend_cap as {{ dbt_utils.type_float() }}) as min_campaign_group_spend_cap,
    cast(extended_credit_invoice_group as {{ dbt_utils.type_float() }}) as extended_credit_invoice_group,
    {{ cast_to_boolean('is_attribution_spec_system_default') }} as is_attribution_spec_system_default,
    {{ cast_to_boolean('is_in_3ds_authorization_enabled_market') }} as is_in_3ds_authorization_enabled_market,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('ad_account_ab1') }}
-- ad_account
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}
