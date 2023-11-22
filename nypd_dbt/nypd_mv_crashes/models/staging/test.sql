SELECT borough, COUNT(DISTINCT(collision_id))
FROM {{source('default','collisions_crashes')}}
WHERE borough is not null
GROUP BY borough
ORDER BY borough ASC