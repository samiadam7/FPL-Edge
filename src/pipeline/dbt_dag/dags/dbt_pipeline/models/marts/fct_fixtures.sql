select
    fix.fix_id,
    fix.season,
    fix.finished,
    home_t.team_name as home_team_name,
    away_t.team_name as away_team_name,
    fix.home_goals,
    fix.away_goals,
    fix.home_team_difficulty,
    fix.away_team_difficulty,
    fix.game_date
from {{ ref('fixtures') }} fix
JOIN {{ ref('team_season_info') }} home_tsi 
    on fix.season = home_tsi.season 
    and fix.home_id = home_tsi.fpl_season_id
JOIN {{ ref('team_season_info') }} away_tsi 
    on fix.season = away_tsi.season 
    and fix.away_id = away_tsi.fpl_season_id
JOIN {{ ref('teams') }} home_t 
    on home_tsi.team_id = home_t.team_id
JOIN {{ ref('teams') }} away_t 
    on away_tsi.team_id = away_t.team_id