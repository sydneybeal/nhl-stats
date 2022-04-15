--select distinct on (full_name)
--team_name,
--full_name,
--sum(points) as points
--from {{ ref('nhl_players') }}  -- or other tables
--order by full_name, sum(points) desc, team_name
--having points > 0

--select team_name, full_name, points from {{ ref('nhl_players') }}

select
team_name,
full_name,
points
from
( select
    team_name,
    full_name,
    sum(points) as points,
    row_number() over(
        partition by team_name
        order by sum(points) desc
    ) as rn
    from {{ ref('nhl_players') }}
    group by team_name, full_name
) d
where d.rn = 1 and points  > 0
order by points desc