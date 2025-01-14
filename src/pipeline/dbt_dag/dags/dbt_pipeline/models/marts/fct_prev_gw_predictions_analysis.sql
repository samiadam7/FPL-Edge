SELECT
    CONCAT(p.player_f_name, ' ', p.player_l_name) AS player_name,
    fix.game_week,
    fix.season,
    rp.goals,
    pgp.predicted_goals,
    rp.assists,
    pgp.predicted_assists
FROM {{ source('fpl_db', 'fct_prev_gw_predictions') }} pgp
JOIN {{ ref('players') }} p ON p.player_id = pgp.player_id
JOIN {{ ref('fixtures') }} fix ON fix.fix_id = pgp.fixture_id
JOIN {{ ref('real_performance') }} rp ON rp.player_id = pgp.player_id AND rp.fix_id = pgp.fixture_id