WITH base AS (
    SELECT DISTINCT
        name AS team_name,
        short_name AS team_short_name,
        fbref_id AS team_fbref_id
    FROM {{ source('fpl_db', 'raw_team_data') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id,
    *
FROM base
