# Facebook Ads
## The Custom DBT Project for Running in Airbyte

The Facebook Marketing source connector performs some default normalization when you
use it to sync data in Airbyte. But we have customized the transformations to better fit the use cases of our partners.

Specifically, the dbt project in this repo ingests data for a subset of the streams
available in the source connector, does some _stuff_, and then delivers that stuff to
materialized views in BigQuery.

## Data Streams Supported

- ad_account
- ad_creatives
- ad_sets
- ads
- ads_insights
- ads_insights_action_type
- campaigns

All streams are configured in Airbyte using Full Refresh Overwrite mode. In theory, at least some
of these may be configured to update incrementally, but let's just say we were having some _difficulties_
and so have given up on that effort for the time being.

## Ad Insights (and why they are tricky)

We expect most organizations will be interested in data from `ads_insights` and `ad_insights_action_type`.
These sources tell you metrics about ads. Things like:

- impressions
- reach
- spend
- thruplays
- cost_per_thruplay
- video_played
- video_p25_watched               
- video_p100_watched

Some of these metrics are easily obtained from `ad_insights`. 
Anything relating to videos, however, must be extracted from arrays within `ad_insights_action_type`.

It's nothing a good `CROSS JOIN UNNEST` can't handle, but you have to do that separately
for each individual metric. That's why, if you look at the models in [models/0_ctes](/models/0_ctes),
you'll see a bunch of (ephemeral) tables to extract some of the different video
metrics into separate tables, which are joined back up with the other ad insights in  
[ads_insights_ab4](/models/0_ctes/ads_insights_ab4.sql).

## dbt Project Structure

All the above nonsense aside, the structure of this dbt project is straightforward.
There are three steps:

1) create the ephemeral tables in [0_ctes](/models/0_ctes)
2) append new data to the base tables in [1_cta_tables](/models/1_cta_tables)
3) define materialized views for target partner in [2_partner_matviews](/models/2_partner_matviews).

Note that if the materialized views already exist, those models are skipped,
and do nothing. But it is still a good thing to have them in there.

## If you want to use this for yourself

You basically just need to find-and-replace some things to get this to work in new contexts.

- [dbt_project.yml](/dbt_project.yml)
- [profiles.yml](profiles.yml)
- [sources.yml](/models/sources.yml)

And you need to find-replace all the source table references in [2_partner_matviews](/models/2_partner_matviews).

## Configuring the custom dbt in Airbyte

You will need to configure three separate "transformations" in the Airbyte sync.

The github repo (with personal access token included) and branch name will be the same for all.

Also make sure to run this on `fishtownanalytics/dbt:1.0.0` (or at least not some earlier version of dbt.

The commands to run in the three transformations are as follows:

1) `deps`
2) `run --profiles-dir=. --target prod-cta --select tag:cta`
3) `run --profiles-dir=. --target prod-partner --select tag:partner`

(Where, of course, `prod-partner` should ideally be changed so that `partner`
refers to the organization on whose behalf this sync is running.)

The targets - `prod-cta` and `prod-partner` - are defined in [profiles.yml](profiles.yml).

The tags - `cta` and `partner` - identify which models to run.
These are configured in [dbt_project.yml](/dbt_project.yml).

### But I don't have partners! I just wanna run this myself

Well then your life is easy! You only need to execute the command: `run --profiles-dir=. --target prod-cta`

(Where, of course, `prod-cta` is instead something more appropriate for your use case)

