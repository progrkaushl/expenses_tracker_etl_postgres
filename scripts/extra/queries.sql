-- Drop raw table and cascade it to drop all the views
DROP TABLE IF EXISTS expenses_raw CASCADE;

CREATE TABLE IF NOT EXISTS expenses_raw
(trans_id char(10), 
trans_dt char(26), 
trans_type char(26), 
trans_amt varchar, 
category VARCHAR, 
trans_cmt varchar, 
trans_src varchar)
;


-- Copy data from the csv file into the table, put filename
COPY expenses_raw FROM '<filename>' DELIMITER ',' CSV HEADER;


-- Create Materialized view to standardize the data types
CREATE MATERIALIZED VIEW IF NOT EXISTS expenses_final_vw AS
SELECT 
    trans_id::numeric AS trans_id, 
    TO_DATE(trans_dt, 'mm/dd/yyyy') AS trans_dt, 
    trans_type, 
    REPLACE(trans_amt,',','')::numeric AS trans_amt, 
    category, 
    trans_cmt, 
    trans_src
FROM expenses_raw;


-- Create view to generate category wise summary
CREATE MATERIALIZED VIEW expenses_cate_summary_vw AS
WITH expenses_summary_1 AS (
    SELECT 
    category, 
    sum(CASE WHEN trans_type = 'Credit' THEN trans_amt ELSE 0 END) as credit,
    sum(CASE WHEN trans_type = 'Debit' THEN trans_amt ELSE 0 END) as debit
    FROM expenses_final_vw 
    GROUP BY category
)
SELECT 
    category,
    credit,
    debit,
    (credit - debit) AS diff
FROM expenses_summary_1 
ORDER BY category ASC
;


-- Create view to generate source wise summary
CREATE MATERIALIZED VIEW expenses_src_summary_vw AS
WITH expenses_summary_1 AS (
    SELECT 
    trans_src, 
    sum(CASE WHEN trans_type = 'Credit' THEN trans_amt ELSE 0 END) as credit,
    sum(CASE WHEN trans_type = 'Debit' THEN trans_amt ELSE 0 END) as debit
    FROM expenses_final_vw 
    GROUP BY trans_src
)
SELECT 
    trans_src,
    credit,
    debit,
    (credit - debit) AS diff
FROM expenses_summary_1 
ORDER BY trans_src ASC
;