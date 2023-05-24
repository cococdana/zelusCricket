

--2b
CREATE VIEW [question2A] AS
SELECT wins.team as team
,wins.year as year
, wins.gender as gender
,wins.wins as wins
,wins.wins * 100.0 / (losses.losses+wins.wins) as win_pct
FROM (
    SELECT year
    , winner as team
    , COUNT(1) as wins,
    gender
    FROM info_table
    GROUP BY year, winner
) as wins
, (
    SELECT year
    , loser as team
    , COUNT(1) as losses
    FROM info_table
    GROUP BY year, loser 
) as losses
WHERE wins.year = losses.year
AND wins.team = losses.team;

select team, win_pct, gender
from [question2A]
group by gender
order by win_pct desc
limit 2;

