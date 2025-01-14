WITH hist_points_comparison AS (
    SELECT
        CONCAT(p.player_f_name, ' ', p.player_l_name) AS player_name,
        fplp.position,
        fix.game_week,
        fix.season,
        CASE
            WHEN fplp.position = 'FWD' THEN (rp.goals * 4 + rp.assists * 3)
            WHEN fplp.position = 'MID' THEN (rp.goals * 5 + rp.assists * 3)
            WHEN fplp.position = 'DEF' THEN (rp.goals * 6 + rp.assists * 3)
        END AS points,
        CASE
            WHEN fplp.position = 'FWD' THEN (GREATEST(pgp.predicted_goals, 0) * 4 + GREATEST(pgp.predicted_assists, 0) * 3)
            WHEN fplp.position = 'MID' THEN (GREATEST(pgp.predicted_goals, 0) * 5 + GREATEST(pgp.predicted_assists, 0) * 3)
            WHEN fplp.position = 'DEF' THEN (GREATEST(pgp.predicted_goals, 0) * 6 + GREATEST(pgp.predicted_assists, 0) * 3)
        END AS predicted_points
    FROM {{ source('fpl_db', 'fct_prev_gw_predictions') }} pgp
    JOIN {{ ref('players') }} p ON p.player_id = pgp.player_id
    JOIN {{ ref('fixtures') }} fix ON fix.fix_id = pgp.fixture_id
    JOIN {{ ref('real_performance') }} rp ON rp.player_id = pgp.player_id AND rp.fix_id = pgp.fixture_id
    JOIN {{ ref('fpl_player') }} fplp ON fplp.player_id = pgp.player_id AND fplp.season = fix.season
),
ranked_players AS (
    SELECT
        player_name,
        position,
        game_week,
        season,
        points,
        predicted_points,
        ROW_NUMBER() OVER (PARTITION BY position, game_week, season ORDER BY predicted_points DESC) AS predicted_rank,
        ROW_NUMBER() OVER (PARTITION BY position, game_week, season ORDER BY points DESC) AS actual_rank
    FROM hist_points_comparison
)
SELECT
    player_name,
    position,
    game_week,
    season,
    points,
    predicted_points,
    predicted_rank,
    actual_rank,
    CASE
        WHEN predicted_rank <= 10 AND actual_rank <= 10 THEN 'Correctly Predicted'
        WHEN predicted_rank <= 10 AND actual_rank > 10 THEN 'False Positive'
        WHEN predicted_rank > 10 AND actual_rank <= 10 THEN 'Missed Prediction'
        ELSE 'Correctly Excluded'
    END AS prediction_accuracy
FROM ranked_players
ORDER BY season, position, game_week, predicted_rank
