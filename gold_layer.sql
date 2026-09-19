-- query for most recent position by train id
CREATE OR REPLACE TABLE streaming_pipeline.trains.most_recent_position AS (
WITH most_recent AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY train_id ORDER BY created_timestamp DESC) AS row_num
FROM streaming_pipeline.trains.train_data_silver
)

SELECT * 
FROM most_recent
WHERE row_num = 1
)
;

-- test queries for most_recent_position 
SELECT * 
FROM streaming_pipeline.trains.most_recent_position
;

SELECT train_id, COUNT(*) 
FROM streaming_pipeline.trains.most_recent_position 
GROUP BY train_id 
HAVING COUNT(*) > 1;


-- suspend silver layer task
ALTER TASK train_schedule_task SUSPEND;

-- most recent position task
CREATE OR REPLACE TASK most_recent_position_task
  WAREHOUSE = TRAIN_PROJECT         
  AFTER STREAMING_PIPELINE.TRAINS.TRAIN_SCHEDULE_TASK          
AS
CREATE OR REPLACE TABLE streaming_pipeline.trains.most_recent_position AS (
WITH most_recent AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY train_id ORDER BY created_timestamp DESC) AS row_num
FROM streaming_pipeline.trains.train_data_silver
)

SELECT * 
FROM most_recent
WHERE row_num = 1
)
;

-- active trains task
CREATE OR REPLACE TASK active_trains_task
  WAREHOUSE = TRAIN_PROJECT         
  AFTER STREAMING_PIPELINE.TRAINS.TRAIN_SCHEDULE_TASK          
AS
  CREATE OR REPLACE TABLE streaming_pipeline.trains.active_trains AS
    SELECT * 
    FROM streaming_pipeline.trains.train_data_silver
    WHERE service_type = 'Normal'
    AND TIMESTAMPDIFF('minute', created_timestamp, current_timestamp()) <= 10 
;

-- aggreate of active train entries
CREATE OR REPLACE TASK active_trains_by_line_task
  WAREHOUSE = TRAIN_PROJECT         
  AFTER STREAMING_PIPELINE.TRAINS.TRAIN_SCHEDULE_TASK
AS
CREATE OR REPLACE TABLE streaming_pipeline.trains.active_trains_by_line AS
    SELECT line, count(DISTINCT train_id)
    FROM streaming_pipeline.trains.train_data_silver
    WHERE service_type = 'Normal'
    AND TIMESTAMPDIFF('minute', created_timestamp, current_timestamp()) <= 10 
    GROUP BY line
;

-- check task dag tree
ALTER TASK active_trains_task RESUME;
ALTER TASK active_trains_by_line_task RESUME;
ALTER TASK most_recent_position_task RESUME;
ALTER TASK train_schedule_task RESUME;

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(TASK_NAME => 'train_schedule_task', RECURSIVE => TRUE));