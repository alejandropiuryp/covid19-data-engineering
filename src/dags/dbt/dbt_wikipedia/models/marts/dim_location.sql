SELECT
    {{ dbt_utils.generate_surrogate_key(['wikipedia_stg.id']) }} as location_key,
    id,
    municipality,
    province,
    coordinates
FROM {{ ref('wikipedia_stg') }}