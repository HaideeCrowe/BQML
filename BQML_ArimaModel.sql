/*
About:
A BQML Model is being created using data pulled from a non-partitioned BQ public dataset, into a private partitioned table that is used in a ML model and DataStudio Reports.

Problem:
The data in the public dataset is randomly updated once monthly - as in the update day is inconsistent
 
Solution:
Create a stored procedure. Running a series of check date queries, to minimize the chances of running a complete table scan every day
Then use the Bigquery Schedular to call the stored procedure daily, comparing the last updated dates.  
 
Considerations:
Reading a tables metadata - process 0 MB of data (however the lastModifiedTime field will be updated with any updates or alterations to the table, including adding labels, tags, columns etc..) 
Reading the metadata of the last partition, on the created partition table - processes 10MB of data
Reading a Select Max(date) from partition table - processes 3.63 MB of data, {this will increase as table size increases}
 
Breakdown of process:
 
If the public data timestamp is > than private tables timestamp
      insert all rows from public table with a greater timestamp, into private table
      If the private table was update
            Run the 'ARIMA' forecasting model with the new data included
Else 
      do nothing and check again the next day
 
*/
-- create stored procedure
CREATE OR REPLACE PROCEDURE test_BQML.append_new_data()
BEGIN
-- create variables
   DECLARE privateTableDate date;
   DECLARE PublicLastDate date;
   DECLARE privateSalesForcast date;
   DECLARE  privateTableDate2 date;
 
--populate above variables - select last date stored in table to be updated
set privateTableDate =
(SELECT MAX(date) FROM `test_BQML.Bottles_sold`);

--select last modified date from public data table [this could have changed with any updates not necessarily append] 0 byte query]
set PublicLastDate = 
(SELECT 
date(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_time 
FROM `bigquery-public-data.iowa_liquor_sales`.__TABLES__ 
where table_id = 'sales');

--only updates rows that date is greater than current max date in my table 
IF (privateTableDate < PublicLastDate) then
      -- check it did actually insert records 0 MB 
      insert into `test_BQML.Bottles_sold`
            SELECT date, item_description, sum(bottles_sold) as bottles_sold
            FROM `bigquery-public-data.iowa_liquor_sales.sales` 
            where date > privateTableDate 
            group by date,item_description;
 
      
      set privateTableDate2 =
      (SELECT 
      date(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_time 
      FROM `test_BQML`.__TABLES__ 
      where table_id = 'Bottles_sold');
    
      -- select last modified date from the forecast models results
      set privateSalesForcast = 
      (SELECT 
      date(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_time 
      FROM `test_BQML`.__TABLES__ 
      where table_id = 'Bottle_sales_forcast');
 
      -- check that the forecasting models output table, last modified date is < private table last modified date, 
      -- incase no data was appended from public table
      IF privateTableDate2 > privateSalesForcast then
        --create forcasting ARIMA (Autoregressive Integrated Moving Average) model - using time series data to predict future trends
        create or replace model `test_BQML.Bottle_sales_forcast`
        Options(
            model_type ='ARIMA',
            time_series_timestamp_col = 'date',
            time_series_data_col = 'bottles_sold',
            time_series_id_col = 'item_description' 
        )
        as 
        SELECT date, item_description, bottles_sold FROM 
        `test_BQML.Bottles_sold`
        where date BETWEEN DATE_TRUNC(privateTableDate2, month) - 730 AND DATE_TRUNC(privateTableDate2, month) + 30;
      END if;
END if;
 
END;

