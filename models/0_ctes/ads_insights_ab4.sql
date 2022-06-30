{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid'
) }}

-- depends_on: {{ ref('ads_insights_ab3') }}, {{ ref('ads_insights_action_type_ab3') }}
select
     insights._airbyte_ads_insights_hashid
    ,insights.cpc
    ,insights.cpm
    ,insights.cpp
    ,insights.ctr
    ,insights.ad_id
    ,insights.reach
    ,insights.spend
    ,insights.clicks
    ,insights.labels
    ,insights.ad_name
    ,insights.adset_id
    ,insights.location
    ,insights.wish_bid
    ,insights.date_stop
    ,insights.frequency
    ,insights.objective
    ,insights.account_id
    ,insights.adset_name
    ,insights.date_start
    ,insights.unique_ctr
    ,insights.auction_bid
    ,insights.buying_type
    ,insights.campaign_id
    ,insights.impressions
    ,insights.account_name
    ,insights.created_time
    ,insights.social_spend
    ,insights.updated_time
    ,insights.age_targeting
    ,insights.campaign_name
    ,insights.unique_clicks
    ,insights.full_view_reach
    ,insights.quality_ranking
    ,insights.account_currency
    ,insights.gender_targeting
    ,insights.optimization_goal
    ,insights.inline_link_clicks
    ,insights.attribution_setting
    ,insights.canvas_avg_view_time
    ,insights.cost_per_unique_click
    ,insights.full_view_impressions
    ,insights.inline_link_click_ctr
    ,insights.estimated_ad_recallers
    ,insights.inline_post_engagement
    ,insights.unique_link_clicks_ctr
    ,insights.auction_competitiveness
    ,insights.canvas_avg_view_percent
    ,insights.conversion_rate_ranking
    ,insights.engagement_rate_ranking
    ,insights.estimated_ad_recall_rate
    ,insights.unique_inline_link_clicks
    ,insights.auction_max_competitor_bid
    ,insights.cost_per_inline_link_click
    ,insights.unique_inline_link_click_ctr
    ,insights.cost_per_estimated_ad_recallers
    ,insights.cost_per_inline_post_engagement
    ,insights.cost_per_unique_inline_link_click
    ,insights.instant_experience_clicks_to_open
    ,insights.estimated_ad_recallers_lower_bound
    ,insights.estimated_ad_recallers_upper_bound
    ,insights.instant_experience_clicks_to_start
    ,insights.estimated_ad_recall_rate_lower_bound
    ,insights.estimated_ad_recall_rate_upper_bound
    ,insights.qualifying_question_qualify_answer_rate
    ,insights._airbyte_ab_id
    ,insights._airbyte_emitted_at
    ,insights._airbyte_normalized_at
    --VIDEO METRICS (UNNESTED)
    ,video_metrics.video_played
    ,video_metrics.video_continuous_2_sec_watched_actions
    ,video_metrics.cost_per_2_sec_continuous_video_view
    ,video_metrics.video_15_sec_watched_actions
    ,video_metrics.cost_per_15_sec_video_view
    ,video_metrics.cost_per_thruplay
    ,video_metrics.video_p25_watched
    ,video_metrics.video_p50_watched
    ,video_metrics.video_p75_watched
    ,video_metrics.video_p95_watched
    ,video_metrics.video_p100_watched
from {{ ref('ads_insights_ab3') }} as insights
LEFT JOIN {{ ref('ads_insights_action_type_ab3') }} as video_metrics
    on insights._airbyte_ads_insights_hashid = video_metrics._airbyte_ads_insights_hashid
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

