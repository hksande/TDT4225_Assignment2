from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine


class QueryOperator:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query1(self):
        query = "SELECT count(*) from User"
        query2 = "SELECT count(*) from Activity"
        query3 = "SELECT count(*) from TrackPoint"
        self.cursor.execute(query)
        response = self.cursor.fetchall()
        self.cursor.execute(query2)
        response2 = self.cursor.fetchall()
        self.cursor.execute(query3)
        response3 = self.cursor.fetchall()
        count_results = [response[0][0], response2[0][0], response3[0][0]]
        print("Query 1:")
        print("Users: ", count_results[0])
        print("Activities: ", count_results[1])
        print("TrackPoints: ", count_results[2])
        self.dotted_line()

    def query2(self):
        query = """SELECT activities.count / users.count
            FROM
            (SELECT COUNT(*) AS count FROM Activity) AS activities,
            (SELECT COUNT(*) AS count FROM User) AS users"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()[0][0]
        print("Query 2:")
        print("Average activities per user: ", round(res, 0))
        self.dotted_line()

    def query3(self):
        query = """SELECT user_id, COUNT(id) AS the_count 
            FROM Activity 
            GROUP BY user_id
            ORDER BY the_count DESC
            LIMIT 20"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 3: Top 20 users with highest number of activities")
        print(tabulate(res, ["UserID", "#Activites"]))

    def query4(self):
        query = """SELECT DISTINCT user_id FROM Activity 
            WHERE transportation_mode = 'taxi'"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 4: Users who have taken a taxi")
        print(tabulate(res, ["UserID"]))
        self.dotted_line()

    def query5(self):
        query = """SELECT transportation, COUNT(id) AS count 
            FROM (
                SELECT transportation_mode as transportation, id FROM Activity 
                WHERE transportation_mode IS NOT NULL 
            ) AS alias
            GROUP BY transportation"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 5: Transportation mode and number of activities with this transportation mode")
        print(tabulate(res, ["Transportation mode", "#Activities"]))
        self.dotted_line()

    def query6a(self):
        query = """SELECT EXTRACT(YEAR FROM start_date_time) AS years, COUNT(*) AS nrActivities 
        FROM Activity 
        WHERE EXTRACT(YEAR FROM start_date_time) >= 2000
        GROUP BY years 
        ORDER BY nrActivities DESC
        LIMIT 1"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 6a: Year with most activities")
        print(tabulate(res, ["Year", "#Activities"]))
        self.dotted_line()


    def query6b(self):
        query = """SELECT EXTRACT(YEAR FROM start_date_time) AS activity_year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS hours
        FROM Activity 
        WHERE EXTRACT(YEAR FROM start_date_time) >= 2000
        GROUP BY EXTRACT(YEAR FROM start_date_time)
        ORDER BY hours DESC
        LIMIT 1"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 6b: Year with most hours in activity")
        print(tabulate(res, ["Year", "Hours"]))
        self.dotted_line()

    def query7(self):
        total_distance = 0

        # First write query to fetch all activity_ids for the given user
        id_query = """
                    SELECT ID
                    FROM Activity
                    WHERE user_id = '112' AND start_date_time LIKE '2008%' 
                    AND end_date_time LIKE '2008%' AND transportation_mode = 'walk'
        """
        self.cursor.execute(id_query)
        activity_ids = self.cursor.fetchall()
        # Map IDs to a normal python list
        activity_ids = list(map(lambda x: x[0], activity_ids))

        # Execute a query for each acitivity_id and calculate distance from first trackpoint to last
        for id in activity_ids[:len(activity_ids)-2]:
            query = """
            SELECT lat, lon FROM TrackPoint WHERE activity_id=%s
            """
            self.cursor.execute(query % id)
            res = self.cursor.fetchall()
            for i in range(0, len(res)-1):
                total_distance += haversine(res[i], res[i+1], unit='km')
        print("Query 7:")
        print("Total distance: ", round(total_distance, 1), "km")
        self.dotted_line()
     
    def query8(self):
        query = """SELECT Sub.UserID, Sub.AltitudeGained 
            FROM ( 
                SELECT Activity.user_id AS userID, 
                       SUM(CASE WHEN TP1.altitude IS NOT NULL AND TP2.altitude IS NOT NULL 
                       THEN (TP2.altitude - TP1.altitude) * 0.0003048 ELSE 0 END) AS AltitudeGained 
                FROM   TrackPoint AS TP1 INNER JOIN TrackPoint AS TP2 ON TP1.activity_id=TP2.activity_id AND 
                       TP1.id+1 = TP2.id INNER JOIN Activity ON Activity.id = TP1.activity_id AND Activity.id = TP2.activity_id 
                WHERE  TP2.altitude > TP1.altitude 
                GROUP  BY Activity.user_id 
                ) AS Sub 
            ORDER BY AltitudeGained DESC LIMIT 20
            """
        self.cursor.execute(query)
        res = self.cursor.fetchall()

        print("Query 8:")
        print(tabulate(res, headers=self.cursor.column_names) + "\n")
        self.dotted_line()

    def query9(self):
        query = """
            SELECT Activity.user_id, COUNT(DISTINCT(ActivityID)) as 'Number of invalid activities' 
            FROM (
                SELECT TP1.activity_id AS ActivityID, (TP2.date_days - TP1.date_days) AS MinuteDiff 
                FROM TrackPoint AS TP1 INNER JOIN TrackPoint AS TP2 
                     ON TP1.activity_id=TP2.activity_id AND TP1.id+1=TP2.id 
                     HAVING MinuteDiff >= 0.00347222
                  ) AS Subtable INNER JOIN Activity ON Activity.id = Subtable.ActivityID 
            GROUP BY Activity.user_id
            """
        self.cursor.execute(query)
        res = self.cursor.fetchall()

        print("Query 9:")
        print(tabulate(res, headers=self.cursor.column_names) + "\n")
        self.dotted_line()

    def query10(self):
        query = """SELECT user_id, lat, lon 
        FROM Activity JOIN TrackPoint ON Activity.id = TrackPoint.activity_id 
        WHERE lat >= 39.916000 AND lat <= 39.916999 
        AND lon >= 116.397000 AND lon <= 116.397999 
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Query 10: Users who have tracked an activity in the forbidden city of Beijing")
        print(tabulate(res, ["User", "Latitude", "Longitude"]))
        self.dotted_line()

    def query11(self):
        #finding the users that have tm labels
        id_query = """SELECT DISTINCT user_id FROM Activity 
            WHERE transportation_mode IS NOT NULL ORDER BY user_id"""
        
        #finding a given users mosted used tm
        query = """SELECT user_id, transportation_mode, count(transportation_mode) as count 
            FROM Activity 
            WHERE transportation_mode IS NOT NULL 
            AND user_id = %s
            GROUP BY user_id, transportation_mode
            ORDER BY user_id, count DESC
            LIMIT 1""" 
        
        self.cursor.execute(id_query)
        id = self.cursor.fetchall()
        ids = list(map(lambda x: x[0], id))

        print("Query 11: Most used transportation mode per user")
        print("Format: [user_id, transportation_mode, times used]")
        for item in ids:
            self.cursor.execute(query % item[0:])
            res = self.cursor.fetchall()
            print(res)
        self.dotted_line()

    def dotted_line(self):
        print("\n" + "*******************************" + "\n")

    def main(self):
        self.query1()
        self.query2()
        self.query3()
        self.query4()
        self.query5()
        self.query6a()
        self.query6b()
        self.query7()
        self.query8()
        self.query9()
        self.query10()
        self.query11()

if __name__ == "__main__":
    try:
        qo = QueryOperator()
        qo.main()
    except Exception as e:
        print(e)
