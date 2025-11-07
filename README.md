ETL Pipeline – My Thought Process, Approch, Challenges, and final output
________________________________________
Understanding the Problem
The task was to design a data pipeline that could process daily sales data from multiple sources like web, mobile, and physical stores.
It had to:
•	Clean invalid and duplicate records
•	Aggregate total sales and revenue per channel
•	Generate reports for daily performance and top 5 products
•	Store everything in a database for future analysis
________________________________________
Thought Process and Approach
1.	Breaking the problem down
I divided the pipeline into simple, modular stages:
o	Extract → Clean → Validate → Load → Aggregate → Report
Each of these became a separate function in my code, so if something broke, I could isolate and fix it quickly.
2.	Data cleaning focus
My cleaning logic handles:
o	Duplicates
o	Invalid records (missing IDs, timestamps, negative values)
o	Removing lines with comments (#) from CSVs
o	Detecting outliers using the IQR method
o	Saving invalid rows separately in the errors/ folder instead of deleting them 
This made the pipeline reliable and auditable — anyone can see what data was rejected and why.
3.	Loading into MySQL
I used SQLAlchemy with PyMySQL as the connector. I built a function to check if required tables exist, and if not, create them automatically.
This ensures the script can be re-run safely without errors.
4.	Aggregation logic
After loading cleaned data into staging_sales, I grouped the data by date and channel to calculate total sales and revenue.
I also calculated top 5 products based on quantity sold.
Both results were written to CSV files and also inserted into a reporting_daily_sales table with an upsert logic — so re-running doesn’t duplicate data.
5.	Reports and traceability
Reports are saved inside the reports/ folder with filenames like:
o	daily_sales_2025-09-12.csv
o	top5_products_2025-09-12.csv
Logs are printed step-by-step so I can trace what happened in every run.
________________________________________
Where I Got Stuck and What Took Time
1.	MySQL Connection Issues
Initially, mysql-connector-python wouldn’t connect properly on my Windows setup, even though the server was running.
I wrote small test scripts (debug_db_connect.py, socket_test.py) to debug it and finally realized the connector was the issue.
Switching to PyMySQL solved everything.
This took a while, but it was a real learning experience in troubleshooting environment issues — something data engineers face often.
2.	Schema mismatch error
After connecting to the DB, I got an error saying:
Unknown column '_source_file' in 'field list'.
This happened because I added a _source_file column during CSV load to track which file each row came from, but that column didn’t exist in the MySQL schema.
I fixed it by renaming _source_file → source_file and adding a whitelist of allowed columns before inserting into MySQL.
This made my load logic safer.
3.	Missing reports issue
At one point, my pipeline ran fine but didn’t generate any reports.
I found out that when the MySQL connection failed, the script exited early, skipping the aggregation step.
I fixed this by adding better exception handling and ensuring that DDL and connection were verified before proceeding.
________________________________________
Database Structure
I designed two main tables:
•	staging_sales → for all cleaned rows (with timestamps and file tracking).
•	reporting_daily_sales → for daily summarized results.
The staging table acts as raw cleaned data, and the reporting table provides ready-to-use metrics.
This pattern is common in production ETL setups and allows incremental updates.
________________________________________
Folder Structure
Interview/

.env

data/raw/         -> Source CSVs

errors/           -> Invalid or rejected rows

reports/          -> Output reports

src/etl_mysql.py  -> Main ETL script
________________________________________
LOG Main lines
INFO | Found 3 CSV files in raw folder

INFO | Removed 1 duplicate row


INFO | Wrote 8 invalid rows to errors/

INFO | Connected to MySQL successfully

INFO | Persisted 10 rows to staging_sales

INFO | Wrote reports: daily_sales_2025-09-12.csv, top5_products_2025-09-12.csv
________________________________________
What Can be done more 
•	Add unit tests for data cleaning functions.
•	Parameterize the date for backfilling reports.
•	Add a simple Airflow DAG or cron job to schedule it daily.
•	Containerize everything with Docker for easier setup.
________________________________________
Code Explanation
1.	Data Ingestion
o	The pipeline reads multiple CSV files from the data/raw folder.
o	Each file may represent a sales channel (web, mobile, or physical store).
o	Comments or extra lines are cleaned before parsing to ensure valid input.
2.	Validation and Cleaning
o	Columns are validated against required fields (timestamp, product_id, etc.).
o	Invalid or negative values are removed, and outliers are filtered using the IQR method.
o	Invalid rows are logged into the errors folder for auditing.
3.	Transformation
o	total_revenue and date columns are derived for easy aggregation later.
4.	Database Integration
o	The cleaned data is loaded into the staging_sales table in MySQL.
o	If the tables don’t exist, they’re automatically created by the script.
o	The connection is made securely using credentials from the .env file.
5.	Aggregation and Reporting
o	Data is grouped by date and channel to calculate total sales and revenue.
o	Top 5 products are identified based on quantity and revenue.
o	Reports are saved in the reports folder and also inserted into reporting_daily_sales in MySQL.
6.	Error Handling and Logging
o	The script uses Python’s logging library to capture all steps, warnings, and errors.
o	This ensures the pipeline can be debugged or extended easily later.
________________________________________
Final Note
This task helped me demonstrate my practical approach to solving real-world data problems.
I took some extra time mainly for debugging MySQL connections and handling schema mismatches, but that process helped me understand the system better.
Overall, I focused on making the pipeline clean, reliable, and close to what I would build in a production setup.
