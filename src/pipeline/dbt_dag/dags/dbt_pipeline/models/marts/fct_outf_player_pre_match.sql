WITH player_performance_rolling AS (
    SELECT 
        CONCAT(p.player_f_name, ' ', p.player_l_name) AS player_name,
        p.player_id,
        fix.fix_id,
        fix.game_week,
        fix.season,
        plays.team_id,

        -- Determine home or away
        CASE 
            WHEN fix.home_id = tsi.fpl_season_id THEN 1
            ELSE 0
        END AS home_or_away,
        
        -- Get the opponent team ID
        CASE 
            WHEN fix.home_id = tsi.fpl_season_id THEN fix.away_id
            ELSE fix.home_id
        END AS opponent_team_id,
        
        -- Calculate team and opponent strength
        (tsi.home_attack_score + tsi.home_defence_score) AS team_strength,
        (opp.away_attack_score + opp.away_defence_score) AS opponent_strength,

        -- Unbounded Rolling Stats
        {{ rolling_stat('real_perf.minutes', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_minutes,
        {{ rolling_stat('real_perf.non_pen_expected_goals', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_np_expected_goals,
        {{ rolling_stat('real_perf.expected_goals', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_expected_goals,
        {{ rolling_stat('real_perf.goals', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_goals,
        {{ rolling_stat('real_perf.expected_assists', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_expected_assists,
        {{ rolling_stat('real_perf.assists', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_assists,
        {{ rolling_stat('real_perf.shots', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_shots,
        {{ rolling_stat('real_perf.shots_on_target', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_shots_on_target,
        {{ rolling_stat('real_perf.pk_made', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_pk_made,
        {{ rolling_stat('real_perf.pk_attempt', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_pk_attempt,
        {{ rolling_stat('real_perf.yellow_card', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_yellow_cards,
        {{ rolling_stat('real_perf.red_card', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_red_cards,
        {{ rolling_stat('real_perf.touches', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_touches,
        {{ rolling_stat('real_perf.tackles', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_tackles,
        {{ rolling_stat('real_perf.interceptions', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_interceptions,
        {{ rolling_stat('real_perf.blocks', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_blocks,
        {{ rolling_stat('real_perf.shot_creating_actions', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_shot_creating_actions,
        {{ rolling_stat('real_perf.goal_creating_actions', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_goal_creating_actions,
        {{ rolling_stat('real_perf.pass_attempted', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_pass_attempted,
        {{ rolling_stat('real_perf.pass_completed', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_pass_completed,
        {{ rolling_stat('real_perf.pass_progressive', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_pass_progressive,
        {{ rolling_stat('real_perf.carries', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_carries,
        {{ rolling_stat('real_perf.carries_progressive', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_carries_progressive,
        {{ rolling_stat('real_perf.take_ons_attempted', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_take_ons_attempted,
        {{ rolling_stat('real_perf.take_ons_success', 'p.player_id, fix.season', 'fix.game_week', 'UNBOUNDED PRECEDING') }} AS rolling_ub_take_ons_success,

        -- Bounded Rolling Stats (Last 3 Fixtures)
        {{ rolling_stat('real_perf.minutes', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_minutes,
        {{ rolling_stat('real_perf.expected_goals', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_expected_goals,
        {{ rolling_stat('real_perf.non_pen_expected_goals', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_np_expected_goals,
        {{ rolling_stat('real_perf.expected_assists', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_expected_assists,
        {{ rolling_stat('real_perf.goals', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_goals,
        {{ rolling_stat('real_perf.assists', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_assists,
        {{ rolling_stat('real_perf.shots', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_shots,
        {{ rolling_stat('real_perf.pk_made', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_pk_made,
        {{ rolling_stat('real_perf.pk_attempt', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_pk_attempt,
        {{ rolling_stat('real_perf.shots_on_target', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_shots_on_target,
        {{ rolling_stat('real_perf.yellow_card', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_yellow_cards,
        {{ rolling_stat('real_perf.red_card', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_red_cards,
        {{ rolling_stat('real_perf.touches', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_touches,
        {{ rolling_stat('real_perf.tackles', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_tackles,
        {{ rolling_stat('real_perf.interceptions', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_interceptions,
        {{ rolling_stat('real_perf.blocks', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_blocks,
        {{ rolling_stat('real_perf.shot_creating_actions', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_shot_creating_actions,
        {{ rolling_stat('real_perf.goal_creating_actions', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_goal_creating_actions,
        {{ rolling_stat('real_perf.pass_completed', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_pass_completed,
        {{ rolling_stat('real_perf.pass_attempted', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_pass_attempted,
        {{ rolling_stat('real_perf.pass_progressive', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_pass_progressive,
        {{ rolling_stat('real_perf.carries', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_carries,
        {{ rolling_stat('real_perf.carries_progressive', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_carries_progressive,
        {{ rolling_stat('real_perf.take_ons_attempted', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_take_ons_attempted,
        {{ rolling_stat('real_perf.take_ons_success', 'p.player_id, fix.season', 'fix.game_week', '3 PRECEDING') }} AS rolling_bound_last3_take_ons_success,

        -- Bounded Rolling Stats (Last 6 Fixtures)
        {{ rolling_stat('real_perf.minutes', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_minutes,
        {{ rolling_stat('real_perf.expected_goals', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_expected_goals,
        {{ rolling_stat('real_perf.non_pen_expected_goals', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_np_expected_goals,
        {{ rolling_stat('real_perf.expected_assists', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_expected_assists,
        {{ rolling_stat('real_perf.goals', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_goals,
        {{ rolling_stat('real_perf.assists', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_assists,
        {{ rolling_stat('real_perf.shots', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_shots,
        {{ rolling_stat('real_perf.pk_made', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_pk_made,
        {{ rolling_stat('real_perf.pk_attempt', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_pk_attempt,
        {{ rolling_stat('real_perf.shots_on_target', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_shots_on_target,
        {{ rolling_stat('real_perf.yellow_card', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_yellow_cards,
        {{ rolling_stat('real_perf.red_card', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_red_cards,
        {{ rolling_stat('real_perf.touches', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_touches,
        {{ rolling_stat('real_perf.tackles', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_tackles,
        {{ rolling_stat('real_perf.interceptions', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_interceptions,
        {{ rolling_stat('real_perf.blocks', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_blocks,
        {{ rolling_stat('real_perf.shot_creating_actions', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_shot_creating_actions,
        {{ rolling_stat('real_perf.goal_creating_actions', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_goal_creating_actions,
        {{ rolling_stat('real_perf.pass_completed', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_pass_completed,
        {{ rolling_stat('real_perf.pass_attempted', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_pass_attempted,
        {{ rolling_stat('real_perf.pass_progressive', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_pass_progressive,
        {{ rolling_stat('real_perf.carries', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_carries,
        {{ rolling_stat('real_perf.carries_progressive', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_carries_progressive,
        {{ rolling_stat('real_perf.take_ons_attempted', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_take_ons_attempted,
        {{ rolling_stat('real_perf.take_ons_success', 'p.player_id, fix.season', 'fix.game_week', '6 PRECEDING') }} AS rolling_bound_last6_take_ons_success,


        -- Efficiency Metrics        
        CASE 
            WHEN rolling_ub_expected_goals > 0 
            THEN rolling_ub_goals / rolling_ub_expected_goals 
            ELSE 0 
        END AS goals_efficiency_ub,

        CASE 
            WHEN rolling_ub_expected_assists > 0 
            THEN rolling_ub_assists / rolling_ub_expected_assists 
            ELSE 0 
        END AS assists_efficiency_ub,

        CASE 
            WHEN rolling_ub_shots > 0 
            THEN rolling_ub_goals / rolling_ub_shots 
            ELSE 0 
        END AS shots_efficiency_ub,

        COALESCE(rolling_ub_goals, 0) + COALESCE(rolling_ub_assists, 0) AS contribution_real_ub,
        COALESCE(rolling_ub_expected_goals, 0) + COALESCE(rolling_ub_expected_assists, 0) AS contribution_xgi_ub,
        COALESCE(rolling_ub_np_expected_goals, 0) + COALESCE(rolling_ub_expected_assists, 0) AS contribution_npxgi_ub,

        CASE 
            WHEN contribution_xgi_ub > 0
            THEN contribution_real_ub / contribution_xgi_ub
            ELSE 0
        END AS contribution_efficiency_xgi_ub,

        CASE 
            WHEN contribution_npxgi_ub > 0
            THEN contribution_real_ub / contribution_npxgi_ub
            ELSE 0
        END AS contribution_efficiency_npxgi_ub,

        CASE 
            WHEN COALESCE(rolling_bound_last3_expected_goals, 0) > 0
            THEN COALESCE(rolling_bound_last3_goals, 0) / COALESCE(rolling_bound_last3_expected_goals, 0)
            ELSE 0
        END AS goals_efficiency_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_expected_assists, 0) > 0
            THEN COALESCE(rolling_bound_last3_assists, 0) / COALESCE(rolling_bound_last3_expected_assists, 0)
            ELSE 0
        END AS assists_efficiency_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_shots, 0) > 0
            THEN COALESCE(rolling_bound_last3_goals, 0) / COALESCE(rolling_bound_last3_shots, 0)
            ELSE 0
        END AS shots_efficiency_last3,

        COALESCE(rolling_bound_last3_goals, 0) + COALESCE(rolling_bound_last3_assists, 0) AS contribution_real_last3,
        COALESCE(rolling_bound_last3_expected_goals, 0) + COALESCE(rolling_bound_last3_expected_assists, 0) AS contribution_xgi_last3,
        COALESCE(rolling_bound_last3_np_expected_goals, 0) + COALESCE(rolling_bound_last3_expected_assists, 0) AS contribution_npxgi_last3,

        CASE 
            WHEN COALESCE(contribution_xgi_last3, 0) > 0
            THEN COALESCE(contribution_real_last3, 0) / COALESCE(contribution_xgi_last3, 0)
            ELSE 0
        END AS contribution_efficiency_xgi_last3,

        CASE 
            WHEN COALESCE(contribution_npxgi_last3, 0) > 0
            THEN COALESCE(contribution_real_last3, 0) / COALESCE(contribution_npxgi_last3, 0)
            ELSE 0
        END AS contribution_efficiency_npxgi_last3,

        -- Unbounded Per-90 Metrics
        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_goals, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS goals_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_assists, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS assists_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(contribution_real_ub, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS real_contribution_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_expected_goals, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS xg_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_np_expected_goals, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS npxg_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_expected_assists, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS xa_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(contribution_xgi_ub, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS xgi_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(contribution_npxgi_ub, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS npxgi_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_shots, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS shots_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_shots_on_target, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS shots_on_target_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_shot_creating_actions, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS shot_creating_actions_per_90_ub,

        CASE 
            WHEN COALESCE(rolling_ub_minutes, 0) > 0 
            THEN COALESCE(rolling_ub_goal_creating_actions, 0) / (COALESCE(rolling_ub_minutes, 0) / 90) 
            ELSE 0 
        END AS goal_creating_actions_per_90_ub,

        -- Bounded Per-90 Metrics (Last 3 Fixtures)
        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_goals, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS goals_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_assists, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS assists_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(contribution_real_last3, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS real_contribution_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_expected_goals, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS xg_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_np_expected_goals, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS npxg_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_expected_assists, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS xa_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(contribution_xgi_last3, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS xgi_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(contribution_npxgi_last3, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS npxgi_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_shots, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS shots_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_shots_on_target, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS shots_on_target_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_shot_creating_actions, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS shot_creating_actions_per_90_last3,

        CASE 
            WHEN COALESCE(rolling_bound_last3_minutes, 0) > 0 
            THEN COALESCE(rolling_bound_last3_goal_creating_actions, 0) / (COALESCE(rolling_bound_last3_minutes, 0) / 90) 
            ELSE 0 
        END AS goal_creating_actions_per_90_last3


    FROM {{ ref('real_performance') }} real_perf
    JOIN {{ ref('players') }} p ON real_perf.player_id = p.player_id
    JOIN {{ ref('fixtures') }} fix ON real_perf.fix_id = fix.fix_id
    JOIN {{ ref('plays') }} ON p.player_id = plays.player_id AND fix.game_date BETWEEN plays.start_date AND COALESCE(plays.end_date, '9999-12-31')
    JOIN {{ ref('team_season_info') }} tsi ON tsi.team_id = plays.team_id AND tsi.season = fix.season
    JOIN {{ ref('team_season_info') }} opp ON opp.team_id = 
        CASE 
            WHEN fix.home_id = tsi.fpl_season_id THEN fix.away_id
            ELSE fix.home_id
        END
        AND opp.season = fix.season
    WHERE real_perf.position != 'GK'
)

SELECT * FROM player_performance_rolling
WHERE rolling_ub_minutes >= 90 -- Exclude rows with insufficient cumulative minutes
