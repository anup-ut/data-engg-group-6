
  
    
    
    
        
         


        insert into `silver`.`silver_merchants`
        ("merchant_id", "acquirer_id", "merchant_name", "state", "created_at", "updated_at")-- dbt_project/models/silver/silver_merchants.sql



WITH src AS (
  SELECT
    toUInt64OrNull(id)                                   AS merchant_id,
    acquirer_id,
    name                                                 AS merchant_name,
    toLowCardinality(state)                              AS state,
    parseDateTimeBestEffortOrNull(created_at)            AS created_at,
    parseDateTimeBestEffortOrNull(updated_at)            AS updated_at
  FROM `bronze`.`merchants`
)
SELECT *
FROM src
WHERE merchant_id IS NOT NULL
  