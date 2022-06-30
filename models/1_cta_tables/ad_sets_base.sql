{% set partitions_to_replace = [
    'timestamp_trunc(current_timestamp, day)',
    'timestamp_trunc(timestamp_sub(current_timestamp, interval 1 day), day)'
] %}

{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "#PARTNER#_facebook_marketing_cta"
) }}
-- Final base SQL model
-- depends_on: {{ ref('ad_sets_ab3') }}
select
     id
    ,name
    ,adlabels
    ,bid_info
    ,end_time
    ,targeting
    ,account_id
    ,start_time
    ,campaign_id
    ,created_time
    ,daily_budget
    ,updated_time
    ,lifetime_budget
    ,promoted_object
    ,budget_remaining
    ,effective_status
    ,_airbyte_ad_sets_hashid
    ,_airbyte_emitted_at
    ,_airbyte_normalized_at
from {{ ref('ad_sets_ab3') }}
-- ad_sets from {{ source('#PARTNER#_facebook_marketing_cta', '_airbyte_raw_ad_sets') }}

{% if is_incremental() %}
where timestamp_trunc(_airbyte_emitted_at, day) in ({{ partitions_to_replace | join(',') }})
{% endif %}