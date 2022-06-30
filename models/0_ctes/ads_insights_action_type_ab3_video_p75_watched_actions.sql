{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid',
    schema = "_airbyte_#PARTNER#_facebook_marketing_cta"
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ads_insights_action_type_ab2') }}

SELECT
     ad_id
    ,adset_id
    ,campaign_id
    ,account_id
    ,date_start
    ,date_stop
    ,video_p25_watched_actions
    ,cast(json_extract_scalar(video_p75_watched_actions, "$['value']") as decimal) as value
    ,_airbyte_ads_insights_hashid
    ,_airbyte_emitted_at
FROM {{ ref('ads_insights_action_type_ab2') }}
CROSS JOIN unnest(video_p75_watched_actions) as video_p75_watched_actions
WHERE 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}
