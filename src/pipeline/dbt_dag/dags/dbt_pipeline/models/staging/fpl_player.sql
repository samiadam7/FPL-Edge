WITH base as (
    SELECT 
        p.player_id,
        temp.element_type AS position,
        temp.season
    FROM {{ ref('players') }} AS p
    JOIN {{ source('fpl_db', 'raw_players_clean') }} AS temp
        ON temp.first_name = p.player_f_name 
        AND temp.second_name = p.player_l_name
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY player_id) AS fpl_player_id,
    *
FROM base