SELECT eventtime, resources
FROM cloudtrail_logs
WHERE  try(resources[1].arn) LIKE 'role'
        AND account = '755428617303'
        LIMIT 10



SELECT account,
        region,
        year,
        month,
        day 
FROM cloudtrail_logs
limit 5







