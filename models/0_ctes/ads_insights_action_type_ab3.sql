{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid',
    schema = "_airbyte_#PARTNER#_facebook_marketing_cta"
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ads_insights_action_type_ab2') }}, {{ ref('ads_insights_action_type_ab3_video_play_actions') }}, {{ ref('ads_insights_action_type_ab3_video_continuous_2_sec_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_cost_per_2_sec_continuous_video_view') }}, {{ ref('ads_insights_action_type_ab3_video_15_sec_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_cost_per_15_sec_video_view') }}, {{ ref('ads_insights_action_type_ab3_cost_per_thruplay') }}, {{ ref('ads_insights_action_type_ab3_video_p25_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_video_p50_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_video_p50_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_video_p75_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_video_p95_watched_actions') }}, {{ ref('ads_insights_action_type_ab3_video_p100_watched_actions') }}

WITH joins as (
SELECT
   base.ad_id
  ,base.adset_id
  ,base.campaign_id
  ,base.account_id
  ,base.date_start
  ,base.date_stop
  ,video_played.value as video_played
  ,video_continuous_2_sec_watched_actions.value as video_continuous_2_sec_watched_actions
  ,cost_per_2_sec_continuous_video_view.value as cost_per_2_sec_continuous_video_view
  ,video_15_sec_watched_actions.value as video_15_sec_watched_actions
  ,cost_per_15_sec_video_view.value as cost_per_15_sec_video_view
  ,cost_per_thruplay.value as cost_per_thruplay
  ,video_p25_watched_actions.value as video_p25_watched
  ,video_p50_watched_actions.value as video_p50_watched
  ,video_p75_watched_actions.value as video_p75_watched
  ,video_p95_watched_actions.value as video_p95_watched
  ,video_p100_watched_actions.value as video_p100_watched
  ,base._airbyte_emitted_at
  ,base._airbyte_normalized_at
FROM {{ ref('ads_insights_action_type_ab2') }} as base
left join {{ ref('ads_insights_action_type_ab3_video_play_actions') }} as video_played
  on video_played._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_continuous_2_sec_watched_actions') }} as video_continuous_2_sec_watched_actions
  on video_continuous_2_sec_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_cost_per_2_sec_continuous_video_view') }} as cost_per_2_sec_continuous_video_view
  on cost_per_2_sec_continuous_video_view._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_15_sec_watched_actions') }} as video_15_sec_watched_actions
  on video_15_sec_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_cost_per_15_sec_video_view') }} as cost_per_15_sec_video_view
  on cost_per_15_sec_video_view._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_cost_per_thruplay') }} as cost_per_thruplay
  on cost_per_thruplay._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_p25_watched_actions') }} as video_p25_watched_actions
  on video_p25_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_p50_watched_actions') }} as video_p50_watched_actions
  on video_p50_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_p75_watched_actions') }} as video_p75_watched_actions
  on video_p75_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_p95_watched_actions') }} as video_p95_watched_actions
  on video_p95_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
left join {{ ref('ads_insights_action_type_ab3_video_p100_watched_actions') }} as video_p100_watched_actions
  on video_p100_watched_actions._airbyte_ads_insights_hashid = base._airbyte_ads_insights_hashid
)

SELECT
    joins.*
    ,{{ dbt_utils.surrogate_key([
        'ad_id',
        'date_start'
    ]) }} as _airbyte_ads_insights_hashid
FROM joins
WHERE 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}
