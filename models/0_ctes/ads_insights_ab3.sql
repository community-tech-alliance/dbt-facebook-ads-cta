{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ads_insights_hashid',
    schema = "_airbyte_#PARTNER#_facebook_marketing_cta"
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ads_insights_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ad_id',
        'date_start'
    ]) }} as _airbyte_ads_insights_hashid
    ,tmp.*
from {{ ref('ads_insights_ab2') }} tmp
-- ads_insights
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

