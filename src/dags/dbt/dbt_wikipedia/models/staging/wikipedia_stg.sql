SELECT
  id,
  municipality,
  province,
  population,
  population_density,
  surface,
  coordinates
FROM
  {{ source('raw_wikipedia', 'SPANISH_MUNICIPALITY') }}
