{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_#PARTNER#_facebook_marketing_cta"
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ads_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'name',
        'adset_id',
        'account_id',
        'campaign_id',
        'created_time',
    ]) }} as _airbyte_ads_hashid,
    tmp.*
from {{ ref('ads_ab2') }} tmp
-- ads
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

