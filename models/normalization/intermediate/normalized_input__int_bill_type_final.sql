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
        , bill.bill_type_code
        , bill.bill_type_description
    from {{ ref('normalized_input__stg_medical_claim') }} as med
    inner join {{ ref('terminology__bill_type') }} as bill
        on {{ ltrim('med.bill_type_code', '0') }} = bill.bill_type_code
    where claim_type = 'institutional'
)

, distinct_counts as (
    select
        claim_id
        , data_source
        , bill_type_code
        , bill_type_description
        , count(*) as bill_type_occurrence_count
    from normalize_cte
    where bill_type_code is not null
    group by
        claim_id
        , data_source
        , bill_type_code
        , bill_type_description
)

, occurence_comparison as (
    select
        claim_id
        , data_source
        , 'bill_type_code' as column_name
        , bill_type_code as normalized_code
        , bill_type_description as normalized_description
        , bill_type_occurrence_count as occurrence_count
        , row_number() over (partition by claim_id, data_source
order by bill_type_occurrence_count desc, bill_type_code asc) as occurrence_row_count
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
    , occurrence_row_count
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from voting_results
where occurrence_row_count = 1
