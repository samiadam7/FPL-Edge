SELECT DISTINCT
    p.player_id,
    fix.fix_id,
    t.Starter,
    t.Pos AS position,
    t.Min AS minutes,
    t.Performance_Gls AS goals,
    t.Performance_Ast AS assists,
    t.Performance_PK AS pk_made,
    t.Performance_PKatt AS pk_attempt,
    t.Performance_Sh AS shots,
    t.Performance_SoT AS shots_on_target,
    t.Performance_CrdY AS yellow_card,
    t.Performance_CrdR AS red_card,
    t.Performance_Touches AS touches,
    t.Performance_Tkl AS tackles,
    t.Performance_Int AS interceptions,
    t.Performance_Blocks AS blocks,
    t.Expected_xG AS expected_goals,
    t.Expected_npxG AS non_pen_expected_goals,
    t.Expected_xAG AS expected_assists,
    t.SCA_SCA AS shot_creating_actions,
    t.SCA_GCA AS goal_creating_actions,
    t.Passes_Cmp AS pass_completed,
    t.Passes_Att AS pass_attempted,
    t.Passes_PrgP AS pass_progressive,
    t.Carries_Carries AS carries,
    t.Carries_PrgC AS carries_progressive,
    t.Take_Ons_Att AS take_ons_attempted,
    t.Take_Ons_Succ AS take_ons_success,
    t.Performance_SoTA AS shot_on_target_against,
    t.Performance_GA AS goals_against,
    t.Performance_Saves AS saves,
    CASE
        WHEN t.Performance_CS = TRUE THEN 1
        ELSE 0
    END AS clean_sheets,
    t.Performance_PSxG AS post_shot_expected_goals,
    t.Captain AS captain
FROM {{ source('fpl_db', 'raw_real_perf') }} AS t
JOIN {{ source('fpl_db', 'raw_compiled_ids') }} AS raw_comp 
    ON raw_comp.fbref_name = t.name
JOIN {{ ref('players') }} AS p 
    ON raw_comp.fpl_f_name = p.player_f_name
    AND raw_comp.fpl_l_name = p.player_l_name
JOIN {{ ref('plays') }} AS plays 
    ON plays.player_id = p.player_id
    AND t.date BETWEEN plays.start_date AND COALESCE(plays.end_date, '9999-12-31')
JOIN {{ ref('team_season_info') }} AS tsi 
    ON plays.team_id = tsi.team_id
    AND t.season = tsi.season
JOIN {{ ref('fixtures') }} AS fix 
    ON t.round = fix.game_week 
    AND t.season = fix.season
    AND (fix.home_id = tsi.fpl_season_id OR fix.away_id = tsi.fpl_season_id)
