{{ config(
     enabled = var('claims_enabled',False)
   )
}}

with summary_keys as (
    select
        data_source
      , table_name
      , claim_type
      , field_name
      , dense_rank() over (
            order by data_source
                   , table_name
                   , claim_type
                   , field_name
        ) as summary_sk
    from (
        select distinct
            data_source
          , table_name
          , claim_type
          , field_name
        from {{ ref('data_quality__data_quality_claims_detail_union') }}
    ) as distinct_keys
)

select
    d.data_source
  , d.source_date
  , d.table_name
  , d.drill_down_key
  , d.drill_down_value
  , d.claim_type
  , d.field_name
  , d.bucket_name
  , d.invalid_reason
  , d.field_value
  , d.tuva_last_run
  , k.summary_sk
from {{ ref('data_quality__data_quality_claims_detail_union') }} as d
inner join summary_keys as k
    on coalesce(d.data_source, '') = coalesce(k.data_source, '')
    and case when d.data_source is null then 1 else 0 end = case when k.data_source is null then 1 else 0 end
    and coalesce(d.table_name, '') = coalesce(k.table_name, '')
    and case when d.table_name is null then 1 else 0 end = case when k.table_name is null then 1 else 0 end
    and coalesce(d.claim_type, '') = coalesce(k.claim_type, '')
    and case when d.claim_type is null then 1 else 0 end = case when k.claim_type is null then 1 else 0 end
    and coalesce(d.field_name, '') = coalesce(k.field_name, '')
    and case when d.field_name is null then 1 else 0 end = case when k.field_name is null then 1 else 0 end
