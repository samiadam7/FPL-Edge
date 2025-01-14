WITH home_teams AS (
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

team_fixtures AS (
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
    JOIN {{ ref('fixtures') }} fix ON fix.fix_id = ts.fix_id
)

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
FROM team_fixtures tf
