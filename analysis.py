import config
import duckdb

def get_run_streaks(connection, minimum_days = 10):
    """
    Retrieve the Streaks greater than a mininum threshold of days. Default = 10.
    """
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

def get_mileage_by_year(connection):
    """
    Retrieve the total mileage by year.
    """
    yearly_mileage = connection.sql("""
       SELECT
            c.Date_Year
            , round(sum(Distance_Miles), 2) AS Total_Distance
        FROM Daily_Log AS d
        INNER JOIN Calendar c ON d.Date = c.Date
        GROUP BY
            c.Date_Year                             
    """)
    print(yearly_mileage)

# Output Analysis
if __name__ == "__main__":
    connection = duckdb.connect(database=config.DATABASE_NAME)
    
    get_run_streaks(connection, minimum_days=10)
    get_mileage_by_year(connection)