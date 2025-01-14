SELECT
    t.team_id,
    temp.season,
    temp.ID AS fpl_season_id,
    temp.strength_attack_home AS home_attack_score,
    temp.strength_defence_home AS home_defence_score,
    temp.strength_attack_away AS away_attack_score,
    temp.strength_defence_away AS away_defence_score
FROM {{ ref('teams') }} AS t
JOIN {{ source('fpl_db', 'raw_team_data') }} AS temp
ON t.team_name = temp.name