SELECT DISTINCT
    fpl.fpl_player_id,
    fix.fix_id,
    temp.total_points AS points,
    temp.value / 10 AS price,
    temp.transfers_in,
    temp.transfers_out,
    temp.bps AS bonus_point_system,
    temp.minutes,
    temp.goals_scored,
    temp.assists,
    temp.clean_sheets,
    temp.goals_conceded,
    temp.own_goals,
    temp.penalties_saved,
    temp.penalties_missed,
    temp.yellow_cards,
    temp.red_cards,
    temp.saves,
    temp.influence AS influence_score,
    temp.creativity AS creativity_score,
    temp.threat AS threat_score,
    temp.ict_index
FROM {{ source('fpl_db', 'raw_fpl_performance') }} temp
JOIN {{ ref('teams') }} t 
    ON t.team_name = temp.team
JOIN {{ ref('team_season_info') }} tsi 
    ON t.team_id = tsi.team_id 
    AND temp.season = tsi.season
JOIN {{ ref('fixtures') }} fix 
    ON DATE(temp.kickoff_time) = DATE(fix.game_date) 
    AND (fix.home_id = tsi.fpl_season_id OR fix.away_id = tsi.fpl_season_id)
JOIN {{ ref('players') }} p 
    ON p.player_f_name = SPLIT_PART(temp.name, ' ', 1) 
    AND p.player_l_name = SPLIT_PART(temp.name, ' ', 2)
JOIN {{ ref('fpl_player') }} fpl 
    ON p.player_id = fpl.player_id 
    AND fpl.season = temp.season
