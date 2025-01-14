WITH max_game_date AS (
    SELECT MAX(game_date) AS max_game_date
    FROM {{ ref('fixtures') }}
),

base AS (
    SELECT 
        p.player_id,
        tsi.team_id,
        MIN(f.game_date) AS start_date,
        CASE 
            WHEN MAX(f.game_date) < (SELECT max_game_date FROM max_game_date)
            THEN MAX(f.game_date)
            ELSE NULL 
        END AS end_date
    FROM {{ source('fpl_db', 'raw_players_clean') }} temp
    JOIN {{ ref('team_season_info') }} tsi 
        ON tsi.fpl_season_id = temp.team AND tsi.season = temp.season
    JOIN {{ ref('fixtures') }} f 
        ON (f.home_id = tsi.fpl_season_id OR f.away_id = tsi.fpl_season_id)
        AND f.season = temp.season
    JOIN {{ ref('players') }} p 
        ON p.player_f_name = temp.first_name AND p.player_l_name = temp.second_name
    GROUP BY
        p.player_id, tsi.team_id
)

SELECT *
FROM base
