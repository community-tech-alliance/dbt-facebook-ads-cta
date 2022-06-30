{{ config(
    cluster_by = "date_start",
    partition_by = {"field": "date_start", "data_type": "date", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid',
    schema = "#PARTNER#_facebook_marketing_cta"
) }}

-- depends_on: {{ ref('ads_insights_ab4') }}
SELECT
     cpc
    ,cpm
    ,cpp
    ,ctr
    ,ad_id
    ,reach
    ,spend
    ,clicks
    ,labels
    ,ad_name
    ,adset_id
    ,location
    ,wish_bid
    ,date_stop
    ,frequency
    ,objective
    ,account_id
    ,adset_name
    ,date_start
    ,unique_ctr
    ,auction_bid
    ,buying_type
    ,campaign_id
    ,impressions
    ,account_name
    ,created_time
    ,social_spend
    ,updated_time
    ,age_targeting
    ,campaign_name
    ,unique_clicks
    ,full_view_reach
    ,quality_ranking
    ,account_currency
    ,gender_targeting
    ,optimization_goal
    ,inline_link_clicks
    ,attribution_setting
    ,canvas_avg_view_time
    ,cost_per_unique_click
    ,full_view_impressions
    ,inline_link_click_ctr
    ,estimated_ad_recallers
    ,inline_post_engagement
    ,unique_link_clicks_ctr
    ,auction_competitiveness
    ,canvas_avg_view_percent
    ,conversion_rate_ranking
    ,engagement_rate_ranking
    ,estimated_ad_recall_rate
    ,unique_inline_link_clicks
    ,auction_max_competitor_bid
    ,cost_per_inline_link_click
    ,unique_inline_link_click_ctr
    ,cost_per_estimated_ad_recallers
    ,cost_per_inline_post_engagement
    ,cost_per_unique_inline_link_click
    ,instant_experience_clicks_to_open
    ,estimated_ad_recallers_lower_bound
    ,estimated_ad_recallers_upper_bound
    ,instant_experience_clicks_to_start
    ,estimated_ad_recall_rate_lower_bound
    ,estimated_ad_recall_rate_upper_bound
    ,qualifying_question_qualify_answer_rate
    ,video_played
    ,video_continuous_2_sec_watched_actions
    ,cost_per_2_sec_continuous_video_view
    ,video_15_sec_watched_actions
    ,cost_per_15_sec_video_view
    ,cost_per_thruplay
    ,video_p25_watched
    ,video_p50_watched
    ,video_p75_watched
    ,video_p95_watched
    ,video_p100_watched
    ,_airbyte_ads_insights_hashid
    ,_airbyte_emitted_at
    ,_airbyte_normalized_at
from {{ ref('ads_insights_ab4') }}

{% if is_incremental() %}
WHERE date_start > (SELECT max(date_start) FROM {{ source('#PARTNER#_facebook_marketing_cta', 'ads_insights_base') }})
{% endif %}