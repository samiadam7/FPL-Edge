WITH next_gw AS (
    SELECT MAX(game_week) AS max_gw
    FROM {{ ref('fixtures') }}
    WHERE finished = TRUE AND season = '2024-25'
),

next_fixture_mapping AS (
    SELECT 
        fix.fix_id AS next_fix_id, 
        tsi.team_id
    FROM {{ ref('fixtures') }} fix
    JOIN {{ ref('team_season_info') }} tsi 
        ON fix.season = tsi.season 
       AND (fix.home_id = tsi.fpl_season_id OR fix.away_id = tsi.fpl_season_id)
    JOIN teams 
        ON teams.team_id = tsi.team_id
    WHERE fix.season = '2024-25' 
      AND fix.game_week = (SELECT max_gw + 1 FROM next_gw)
),

home_teams AS (
    SELECT
        fix.fix_id,
        fix.season,
        fix.game_week,
        fix.home_id AS fpl_season_id,
        'home' AS match_location
    FROM {{ ref('fixtures') }} fix
),
away_teams AS (
    SELECT
        fix.fix_id,
        fix.season,
        fix.game_week,
        fix.away_id AS fpl_season_id,
        'away' AS match_location
    FROM {{ ref('fixtures') }} fix
),
team_stats AS (
    SELECT 
        ht.fix_id,
        ht.season,
        ht.game_week,
        ht.match_location,
        tsi.team_id,
        tsi.fpl_season_id
    FROM home_teams ht
    JOIN {{ ref('team_season_info') }} tsi
        ON ht.fpl_season_id = tsi.fpl_season_id
        AND ht.season = tsi.season
    
    UNION ALL
    
    SELECT 
        at.fix_id,
        at.season,
        at.game_week,
        at.match_location,
        tsi.team_id,
        tsi.fpl_season_id
    FROM away_teams at
    JOIN {{ ref('team_season_info') }} tsi
        ON at.fpl_season_id = tsi.fpl_season_id
        AND at.season = tsi.season
),
all_team_fixtures AS (
    -- Include all fixtures up to and including the next game week
    SELECT
        ts.team_id,
        fix.fix_id,
        fix.season,
        fix.game_week,
        ts.match_location,
        CASE 
            WHEN ts.match_location = 'home' THEN fix.home_goals
            ELSE fix.away_goals
        END AS team_goals_scored,
        CASE 
            WHEN ts.match_location = 'home' THEN fix.away_goals
            ELSE fix.home_goals
        END AS team_goals_conceded
    FROM team_stats ts
    JOIN {{ ref('fixtures') }} fix
        ON fix.fix_id = ts.fix_id
    WHERE fix.season = '2024-25'
      AND fix.game_week <= (SELECT max_gw + 1 FROM next_gw)
),
rolling_team_stats AS (
    -- Calculate rolling stats for all fixtures
    SELECT
        tf.team_id,
        tf.fix_id,
        tf.season,
        tf.game_week,
        tf.match_location,
        {{ rolling_stat('team_goals_scored', 'team_id, season', 'game_week', '3 PRECEDING') }} AS rolling_goals_scored_last3,
        {{ rolling_stat('team_goals_conceded', 'team_id, season', 'game_week', '3 PRECEDING') }} AS rolling_goals_conceded_last3,
        {{ rolling_stat('team_goals_scored', 'team_id, season', 'game_week', '6 PRECEDING') }} AS rolling_goals_scored_last6,
        {{ rolling_stat('team_goals_conceded', 'team_id, season', 'game_week', '6 PRECEDING') }} AS rolling_goals_conceded_last6,
        {{ rolling_stat('team_goals_scored', 'team_id, season', 'game_week', 'UNBOUNDED PRECEDING') }} AS rolling_goals_scored_ub,
        {{ rolling_stat('team_goals_conceded', 'team_id, season', 'game_week', 'UNBOUNDED PRECEDING') }} AS rolling_goals_conceded_ub
    FROM all_team_fixtures tf
)

-- Select only the next fixture and its rolling stats
SELECT 
    rts.*
FROM rolling_team_stats rts
JOIN next_fixture_mapping nfm
    ON rts.team_id = nfm.team_id
WHERE rts.game_week = (SELECT max_gw + 1 FROM next_gw)
