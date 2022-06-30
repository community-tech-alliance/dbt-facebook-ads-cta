{% set partitions_to_replace = [
    'timestamp_trunc(current_timestamp, day)',
    'timestamp_trunc(timestamp_sub(current_timestamp, interval 1 day), day)'
] %}

{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_hashid',
    schema = "#PARTNER#_facebook_marketing_cta"
) }}

-- Final base SQL model
-- depends_on: {{ ref('ads_ab3') }}
select
     JSON_EXTRACT_SCALAR(creative, "$.id") as creative_id
    ,id
    ,name
    ,status
    ,adlabels
    ,adset_id
    ,bid_info
    ,bid_type
    ,creative
    ,targeting
    ,account_id
    ,bid_amount
    ,campaign_id
    ,created_time
    ,source_ad_id
    ,updated_time
    ,tracking_specs
    ,recommendations
    ,conversion_specs
    ,effective_status
    ,last_updated_by_app_id
    ,_airbyte_ads_hashid
    ,_airbyte_emitted_at
    ,_airbyte_normalized_at
from {{ ref('ads_ab3') }}

{% if is_incremental() %}
where timestamp_trunc(_airbyte_emitted_at, day) in ({{ partitions_to_replace | join(',') }})
{% endif %}
