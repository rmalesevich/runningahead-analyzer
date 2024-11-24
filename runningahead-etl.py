import config
import shutil
import time, os, zipfile
import duckdb

from selenium import webdriver
from selenium.webdriver.common.by import By

# Cleanup any files on the file system
def cleanup_filesystem(zip_file, extract_folder):
    """
    This function deletes the ZIP file (if it exists) and any extracted files from the ZIP file.
    """
    if os.path.exists(zip_file):
        os.remove(zip_file)
    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)

# Export the log details from RunningAHEAD
def download_log_from_ra(ra_user, ra_pass, ra_id):
    """
    This function uses Selenium, specifically with the Chrome Webdriver to navigate the RunningAHEAD.com website to:
    - Log in with the passed in username and password
    - Navigate to the Export tools and executing the submit button.

    This will download a ZIP file to the location that Chrome defaults to (~/Downloads for example)
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    driver.get("https://www.runningahead.com/")

    driver.find_element(By.ID, "ctl00_ctl00_ctl00_SiteContent_PageContent_MainContent_email").send_keys(ra_user)
    driver.find_element(By.ID, "ctl00_ctl00_ctl00_SiteContent_PageContent_MainContent_password").send_keys(ra_pass)
    driver.find_element(By.ID, "ctl00_ctl00_ctl00_SiteContent_PageContent_MainContent_login_s").click()

    driver.get("https://www.runningahead.com/logs/" + ra_id + "/tools/export")
    driver.find_element(By.ID, "ctl00_ctl00_ctl00_SiteContent_PageContent_TrainingLogContent_Download_s").click()

    time.sleep(10)
    driver.quit()

# Preprocess the ZIP File
def preprocess_ra_file(zip_file, extract_folder):
    """
    The file downloaded from RunningAHEAD must be unzipped and then cleaned because the header of the TSV file contains an extra tab character.
    """
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

    log_file = extract_folder + "log.txt"
    with open(log_file, "r") as f:
        lines = f.readlines()
    
    new_header = ''.join(lines[0].rsplit("\t", 1))

    with open(log_file, "w") as f:
        f.write(new_header)

        count = 0
        for line in lines:
            if count > 0:
                f.write(line + "\n")
            count = count + 1

    return log_file

def process_to_duckdb(db_file, log_file):
    """
    The ETL processor will re-create the database each time. The volume of data is minimal. It was determined, it's not worth the overhead to use delta loading.
    """
    connection = duckdb.connect(database=db_file)

    # Create the Conversion Table
    connection.sql("""
        CREATE OR REPLACE TABLE Unit_Conversion AS
            SELECT 'Mile' AS Unit, 1 AS Conversion_Factor
            UNION ALL
            SELECT 'Kilometer', 0.6213712
            UNION ALL
            SELECT 'Meter', 0.0006213712
        """
    )

    # Store the TSV data to the primary log table
    connection.sql("""
        CREATE OR REPLACE TABLE Log AS
            SELECT
                l.Date
                , l.SubType AS Category
                , l.Distance
                , l.DistanceUnit AS Distance_Unit
                , l.Distance * COALESCE(u.Conversion_Factor, 0) AS Distance_Miles
                , (date_part('hour', l.Duration) * 3600) + (date_part('minute', l.Duration) * 60) + (date_part('second', l.Duration)) AS Duration_Seconds
            FROM read_csv('{0}') AS l
            LEFT JOIN Unit_Conversion u ON l.DistanceUnit = u.Unit
            WHERE Type = 'Run'
    """.format(log_file))

    # Create a Daily Log Table
    connection.sql("""
        CREATE OR REPLACE TABLE Daily_Log AS
            WITH aggregated_log AS (
                SELECT
                    Date
                    , SUM(Distance_Miles) AS Distance_Miles
                    , SUM(Duration_Seconds) AS Duration_Seconds
                FROM Log
                GROUP BY
                    Date
            ), streak_breaks AS (
                SELECT
                    Date
                    , Distance_Miles
                    , Duration_Seconds
                    , CASE
                        WHEN Date - 1 = LAG(Date) OVER (ORDER BY Date) AND Distance_Miles >= 1 THEN 0
                        ELSE 1
                    END AS Streak_Break
                FROM aggregated_log
            ), streak_groups AS (
                SELECT
                    Date
                    , Distance_Miles
                    , Duration_Seconds
                    , SUM(Streak_Break) OVER (ORDER BY Date) AS Streak_Group
                FROM streak_breaks
            )
            SELECT
                Date
                , Streak_Group AS Streak_ID
                , ROW_NUMBER() OVER (PARTITION BY Streak_Group ORDER BY Date) AS Streak_Count
                , Distance_Miles
                , Duration_Seconds
            FROM streak_groups;
    """)

    # Create a Calendar Table to assist in future analysis
    connection.sql("""
        CREATE OR REPLACE TABLE Calendar AS
            WITH RECURSIVE seq(n) AS (
                SELECT
                    0
                UNION ALL
                SELECT
                    n + 1
                FROM seq
                WHERE n < datediff('day', (SELECT MIN(Date) FROM Daily_Log), today())
            ), d(d) AS (
                SELECT
                    date_add((SELECT MIN(Date) FROM Daily_Log), n)
                FROM seq
            )
            SELECT
                 d AS Date
                , year(d) AS Date_Year
                , month(d) AS Date_Month
                , day(d) AS Date_Day
                , dayofyear(d) AS Date_Day_of_Year
                , dayofmonth(d) AS Date_Day_of_Month
                , dayofweek(d) AS Date_Day_of_Week
                , week(d) AS Date_Week
                , weekofyear(d) AS Date_Week_of_Year
                , yearweek(d) AS Date_Year_Week
                , quarter(date) AS Date_Quarter
            FROM d
    """)

def get_run_streaks(db_file, minimum_days = 10):
    """
    Temporary function to output the streaks greater than the minimum_days.
    """
    connection = duckdb.connect(database=db_file)

    streaks = connection.sql("""
        SELECT
            Streak_ID
            , min(Date) AS Start_of_Streak
            , max(Date) AS Last_Day_of_Streak
            , round(sum(Distance_Miles), 2) AS Total_Distance
            , max(Streak_Count) AS Total_Days
        FROM Daily_Log
        GROUP BY
            Streak_ID
        HAVING total_days > {0}
        ORDER BY Total_Days DESC, Last_Day_of_Streak ASC
        """.format(minimum_days)
    )
    print(streaks)

# ETL Controller
if __name__ == "__main__":    
    cleanup_filesystem(config.DOWNLOAD_FILE, config.EXTRACT_FOLDER)
    
    download_log_from_ra(config.RA_USER, config.RA_PASS, config.RA_ID)

    log_file = preprocess_ra_file(config.DOWNLOAD_FILE, config.EXTRACT_FOLDER)
    process_to_duckdb(config.DATABASE_NAME, log_file)

    cleanup_filesystem(config.DOWNLOAD_FILE, config.EXTRACT_FOLDER)

    get_run_streaks(config.DATABASE_NAME, minimum_days=10)