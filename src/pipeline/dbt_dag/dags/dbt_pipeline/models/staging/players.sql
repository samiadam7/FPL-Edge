WITH base AS (
    SELECT DISTINCT
        fpl_f_name AS player_f_name,
        fpl_l_name AS player_l_name,
        id_fbref AS fbref_id
    FROM {{ source('fpl_db', 'raw_compiled_ids') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY player_f_name, player_l_name) AS player_id,
    *
FROM base
