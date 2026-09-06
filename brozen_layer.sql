-- set up
CREATE WAREHOUSE train_project WITH WAREHOUSE_SIZE = 'XSMALL', AUTO_RESUME = TRUE, AUTO_SUSPEND = 300;
CREATE DATABASE streaming_pipeline;
CREATE SCHEMA streaming_pipeline.trains;
ALTER USER EBARTELS SET RSA_PUBLIC_KEY = 'PUBLIC_KEY';

-- old table schema
CREATE OR REPLACE TABLE streaming_pipeline.trains.train_data_raw (
    record_key STRING,
    value VARIANT,
    ts timestamp DEFAULT CURRENT_TIMESTAMP()
)
;

-- verification
SELECT current_user();
SELECT CURRENT_ROLE();

-- final testing for data flowing
SELECT * FROM streaming_pipeline.trains.train_data_raw ORDER BY 1 DESC LIMIT 20;