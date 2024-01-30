SELECT
    {{ dbt_utils.generate_surrogate_key(['wikipedia_stg.id']) }} as location_key,
    id,
    population_density,
    surface,
    population
FROM {{ ref('wikipedia_stg') }}