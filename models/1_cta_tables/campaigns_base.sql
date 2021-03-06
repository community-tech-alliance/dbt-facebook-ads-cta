{% set partitions_to_replace = [
    'timestamp_trunc(current_timestamp, day)',
    'timestamp_trunc(timestamp_sub(current_timestamp, interval 1 day), day)'
] %}

{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_campaigns_hashid',
    schema = "#PARTNER#_facebook_marketing_cta"
) }}
-- Final base SQL model
-- depends_on: {{ ref('campaigns_ab3') }}
select
     id
    ,name
    ,adlabels
    ,objective
    ,spend_cap
    ,stop_time
    ,account_id
    ,start_time
    ,buying_type
    ,issues_info
    ,bid_strategy
    ,created_time
    ,daily_budget
    ,updated_time
    ,lifetime_budget
    ,budget_remaining
    ,effective_status
    ,source_campaign_id
    ,special_ad_category
    ,smart_promotion_type
    ,budget_rebalance_flag
    ,special_ad_category_country
    ,_airbyte_campaigns_hashid
    ,_airbyte_emitted_at
    ,_airbyte_normalized_at
from {{ ref('campaigns_ab3') }}
-- campaigns from {{ source('#PARTNER#_facebook_marketing_cta', '_airbyte_raw_campaigns') }}

{% if is_incremental() %}
where timestamp_trunc(_airbyte_emitted_at, day) in ({{ partitions_to_replace | join(',') }})
{% endif %}