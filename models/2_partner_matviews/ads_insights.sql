{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid',
    schema = "facebook_marketing"
) }}

-- depends_on: {{ source('#PARTNER#_facebook_marketing_cta', 'ads_insights_base') }}

SELECT
     _airbyte_ads_insights_hashid
    ,MAX(cpc) as cpc
    ,MAX(cpm) as cpm
    ,MAX(cpp) as cpp
    ,MAX(ctr) as ctr
    ,MAX(ad_id) as ad_id
    ,MAX(reach) as reach
    ,MAX(spend) as spend
    ,MAX(clicks) as clicks
    ,MAX(labels) as labels
    ,MAX(ad_name) as ad_name
    ,MAX(adset_id) as adset_id
    ,MAX(location) as location
    ,MAX(wish_bid) as wish_bid
    ,MAX(date_stop) as date_stop
    ,MAX(frequency) as frequency
    ,MAX(objective) as objective
    ,MAX(account_id) as account_id
    ,MAX(adset_name) as adset_name
    ,MAX(date_start) as date_start
    ,MAX(unique_ctr) as unique_ctr
    ,MAX(auction_bid) as auction_bid
    ,MAX(buying_type) as buying_type
    ,MAX(campaign_id) as campaign_id
    ,MAX(impressions) as impressions
    ,MAX(account_name) as account_name
    ,MAX(created_time) as created_time
    ,MAX(social_spend) as social_spend
    ,MAX(updated_time) as updated_time
    ,MAX(age_targeting) as age_targeting
    ,MAX(campaign_name) as campaign_name
    ,MAX(unique_clicks) as unique_clicks
    ,MAX(full_view_reach) as full_view_reach
    ,MAX(quality_ranking) as quality_ranking
    ,MAX(account_currency) as account_currency
    ,MAX(gender_targeting) as gender_targeting
    ,MAX(optimization_goal) as optimization_goal
    ,MAX(inline_link_clicks) as inline_link_clicks
    ,MAX(attribution_setting) as attribution_setting
    ,MAX(canvas_avg_view_time) as canvas_avg_view_time
    ,MAX(cost_per_unique_click) as cost_per_unique_click
    ,MAX(full_view_impressions) as full_view_impressions
    ,MAX(inline_link_click_ctr) as inline_link_click_ctr
    ,MAX(estimated_ad_recallers) as estimated_ad_recallers
    ,MAX(inline_post_engagement) as inline_post_engagement
    ,MAX(unique_link_clicks_ctr) as unique_link_clicks_ctr
    ,MAX(auction_competitiveness) as auction_competitiveness
    ,MAX(canvas_avg_view_percent) as canvas_avg_view_percent
    ,MAX(conversion_rate_ranking) as conversion_rate_ranking
    ,MAX(engagement_rate_ranking) as engagement_rate_ranking
    ,MAX(estimated_ad_recall_rate) as estimated_ad_recall_rate
    ,MAX(unique_inline_link_clicks) as unique_inline_link_clicks
    ,MAX(auction_max_competitor_bid) as auction_max_competitor_bid
    ,MAX(cost_per_inline_link_click) as cost_per_inline_link_click
    ,MAX(unique_inline_link_click_ctr) as unique_inline_link_click_ctr
    ,MAX(cost_per_estimated_ad_recallers) as cost_per_estimated_ad_recallers
    ,MAX(cost_per_inline_post_engagement) as cost_per_inline_post_engagement
    ,MAX(cost_per_unique_inline_link_click) as cost_per_unique_inline_link_click
    ,MAX(instant_experience_clicks_to_open) as instant_experience_clicks_to_open
    ,MAX(estimated_ad_recallers_lower_bound) as estimated_ad_recallers_lower_bound
    ,MAX(estimated_ad_recallers_upper_bound) as estimated_ad_recallers_upper_bound
    ,MAX(instant_experience_clicks_to_start) as instant_experience_clicks_to_start
    ,MAX(estimated_ad_recall_rate_lower_bound) as estimated_ad_recall_rate_lower_bound
    ,MAX(estimated_ad_recall_rate_upper_bound) as estimated_ad_recall_rate_upper_bound
    ,MAX(qualifying_question_qualify_answer_rate) as qualifying_question_qualify_answer_rate
    ,MAX(video_played) as video_played
    ,MAX(video_continuous_2_sec_watched_actions) as video_continuous_2_sec_watched_actions
    ,MAX(cost_per_2_sec_continuous_video_view) as cost_per_2_sec_continuous_video_view
    ,MAX(video_15_sec_watched_actions) as video_15_sec_watched_actions
    ,MAX(cost_per_15_sec_video_view) as cost_per_15_sec_video_view
    ,MAX(video_15_sec_watched_actions) as thruplays
    ,MAX(cost_per_thruplay) as cost_per_thruplay
    ,MAX(video_p25_watched) as video_p25_watched
    ,MAX(video_p50_watched) as video_p50_watched
    ,MAX(video_p75_watched) as video_p75_watched
    ,MAX(video_p95_watched) as video_p95_watched
    ,MAX(video_p100_watched) as video_p100_watched
    ,MAX(_airbyte_emitted_at) as _airbyte_emitted_at
    ,MAX(_airbyte_normalized_at) as _airbyte_normalized_at
FROM {{ source('#PARTNER#_facebook_marketing_cta', 'ads_insights_base') }}
GROUP BY _airbyte_ads_insights_hashid