WITH base AS (
    SELECT
        season,
        event AS game_week,
        finished,
        team_h_score AS home_goals,
        team_a_score AS away_goals,
        kickoff_time AS game_date,
        team_h AS home_id,
        team_a AS away_id,
        team_h_difficulty AS home_team_difficulty,
        team_a_difficulty AS away_team_difficulty
    FROM {{ source('fpl_db', 'raw_fixtures') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY season, game_week, home_id, away_id) AS fix_id,
    *
FROM base