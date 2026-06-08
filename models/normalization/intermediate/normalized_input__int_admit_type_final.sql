{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

-- The voting step (previously a separate `_voting` model) is inlined
-- here as a CTE; this model applies the winning-vote filter on top of it.


with normalize_cte as (
    select
        med.claim_id
        , med.data_source
        , admit.admit_type_code
        , admit.admit_type_description
    from {{ ref('normalized_input__stg_medical_claim') }} as med
    inner join {{ ref('terminology__admit_type') }} as admit
        on med.admit_type_code = admit.admit_type_code
    where claim_type = 'institutional'
)

, distinct_counts as (
    select
        claim_id
        , data_source
        , admit_type_code
        , admit_type_description
        , count(*) as admit_type_occurrence_count
    from normalize_cte

    where admit_type_code is not null
    group by
        claim_id
        , data_source
        , admit_type_code
        , admit_type_description
)

, occurence_comparison as (
    select
        claim_id
        , data_source
        , 'admit_type_code' as column_name
        , admit_type_code as normalized_code
        , admit_type_description as normalized_description
        , admit_type_occurrence_count as occurrence_count
        , coalesce(lead(admit_type_occurrence_count)
            over (partition by claim_id, data_source
order by admit_type_occurrence_count desc), 0) as next_occurrence_count
        , row_number() over (partition by claim_id, data_source
order by admit_type_occurrence_count desc) as occurrence_row_count
    from distinct_counts as dist
)

, voting_results as (

select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , normalized_description
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from occurence_comparison

)

select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , normalized_description
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from voting_results
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
