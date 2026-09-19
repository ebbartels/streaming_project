-- create train stream
CREATE OR REPLACE STREAM streaming_pipeline.trains.train_stream ON TABLE streaming_pipeline.trains.train_data_raw;

-- test stream
SELECT * 
FROM train_stream;

-- test table, dropped
CREATE OR REPLACE TEMP TABLE train_stream_test AS
SELECT * 
FROM train_stream
;

-- silver table schema, table gets populated during task below
CREATE TABLE streaming_pipeline.trains.train_data_silver ( 
train_id string,
 line string,
 train_number string,
 circuit_id int,
 cars int,
 seconds_at_location int,
 service_type string,
 destination string,
 direction int,
 created_timestamp timestamp,
 processed_at timestamp DEFAULT CURRENT_TIMESTAMP()
);

-- test select casting
SELECT record_content:CarCount::INTEGER, record_content:CircuitId::INTEGER, record_content:DestinationStationCode::STRING ,record_content:DirectionNum::INTEGER, record_content:LineCode::STRING, record_content:SecondsAtLocation::INTEGER, record_content:ServiceType::STRING, record_content:TrainId::STRING, record_content:TrainNumber::STRING, TO_TIMESTAMP(record_metadata:CreateTime::BIGINT, 3)
FROM train_stream
LIMIT 10
;

-- test inserting
INSERT INTO train_data_silver (cars, circuit_id, destination, direction, line, seconds_at_location, service_type, train_id, train_number, created_timestamp)
SELECT record_content:CarCount::INTEGER, record_content:CircuitId::INTEGER, record_content:DestinationStationCode::STRING ,record_content:DirectionNum::INTEGER, record_content:LineCode::STRING, record_content:SecondsAtLocation::INTEGER, record_content:ServiceType::STRING, record_content:TrainId::STRING, record_content:TrainNumber::STRING, TO_TIMESTAMP(record_metadata:CreateTime::BIGINT, 3)
FROM train_stream
;

-- test query for insertion
SELECT * FROM streaming_pipeline.trains.train_data_silver ORDER BY created_timestamp DESC LIMIT 20;

-- create task to insert
CREATE OR REPLACE TASK train_schedule_task
  WAREHOUSE = TRAIN_PROJECT         
  SCHEDULE = '5 MINUTE'          
  WHEN SYSTEM$STREAM_HAS_DATA('train_stream')
AS
  INSERT INTO train_data_silver (cars, circuit_id, destination, direction, line, seconds_at_location, service_type, train_id, train_number, created_timestamp)
    SELECT record_content:CarCount::INTEGER, record_content:CircuitId::INTEGER, record_content:DestinationStationCode::STRING ,record_content:DirectionNum::INTEGER, record_content:LineCode::STRING, record_content:SecondsAtLocation::INTEGER, record_content:ServiceType::STRING, record_content:TrainId::STRING, record_content:TrainNumber::STRING, TO_TIMESTAMP(record_metadata:CreateTime::BIGINT, 3)
    FROM train_stream
;

-- check the task
ALTER TASK train_schedule_task RESUME;

SHOW TASKS;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- check for rows being added
--12993	2026-09-14 18:13:46.792
SELECT COUNT(*), MAX(processed_at) 
FROM streaming_pipeline.trains.train_data_silver;