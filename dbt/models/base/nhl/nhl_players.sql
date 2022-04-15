select
  nhl_player_id as id,
  full_name as full_name,
  max(game_team_name) as team_name,
  sum(stats_assists) as assists,
  sum(stats_goals) as goals,
  sum(stats_assists+stats_goals) as points
from {{ ref('player_game_stats') }}
where side != 'error'
group by id, full_name
