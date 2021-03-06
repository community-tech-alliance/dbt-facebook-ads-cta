{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_account_report_hashid',
    schema = "facebook_marketing_qc"
) }}

-- depends_on: {{ source('#PARTNER#_facebook_marketing_cta', 'account_report_agg') }}

SELECT
     _account_report_hashid
    ,MAX(date_start) as date_start
    ,MAX(account_id) as account_id
    ,MAX(account_name) as account_name
    ,MAX(clicks) as clicks
    ,MAX(impressions) as impressions
    ,MAX(spend) as spend
    ,MAX(_airbyte_emitted_at) as _airbyte_emitted_at
    ,MAX(_airbyte_normalized_at) as _airbyte_normalized_at
FROM {{ source('#PARTNER#_facebook_marketing_cta', 'account_report_agg') }}
GROUP BY _account_report_hashid