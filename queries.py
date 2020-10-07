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
        print("Users: ", count_results[0])
        print("Activities: ", count_results[1])
        print("TrackPoints: ", count_results[2])

    def query2(self):
        query = """SELECT activities.count / users.count
            FROM
            (SELECT COUNT(*) AS count FROM Activity) AS activities,
            (SELECT COUNT(*) AS count FROM User) AS users"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()[0][0]
        print("Average activities per user: ", round(res, 0))

    def query3(self):
        query = """SELECT user_id, COUNT(id) AS the_count 
            FROM Activity 
            GROUP BY user_id
            ORDER BY the_count DESC
            LIMIT 20"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Top 20 users with highest number of activities")
        print(tabulate(res, ["UserID", "#Activites"]))

    def query4(self):
        query = """SELECT DISTINCT user_id FROM Activity 
            WHERE transportation_mode = 'taxi'"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print(tabulate(res, ["Users who have taken a taxi"]))

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
        print(tabulate(res, ["Transportation mode", "#Activity"]))

    def query6a(self):
        query = """SELECT EXTRACT(YEAR FROM start_date_time) AS years, COUNT(*) AS nrActivities 
        FROM Activity 
        WHERE EXTRACT(YEAR FROM start_date_time) >= 2000
        GROUP BY years 
        ORDER BY nrActivities DESC
        LIMIT 1"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print(tabulate(res, ["Year with most activities", "nrActivities"]))


    def query6b(self):
        query = """SELECT EXTRACT(YEAR FROM start_date_time) AS activity_year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS hours
        FROM Activity 
        WHERE EXTRACT(YEAR FROM start_date_time) >= 2000
        GROUP BY EXTRACT(YEAR FROM start_date_time)
        ORDER BY hours DESC
        LIMIT 1"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print(tabulate(res, ["Year with most hours in activity", "Hours"]))

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
        print("Total distance: ", round(total_distance, 1), "km")

     
      def query11(self):
        query = """SELECT user_id, transportation_mode, count(transportation_mode) as count 
        FROM Activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY user_id, transportation_mode
        ORDER BY user_id, count DESC""" 
        #får dataen jeg trenger sortert for å kunne se det manuelt, 
        # men ikke funnet en måte å hente ut tm med høyest count per user_id

    def main(self):
        # self.query1()
        # self.query2()
        # self.query3()
        # self.query4()
        self.query5()


if __name__ == "__main__":
    try:
        qo = QueryOperator()
        qo.main()
    except Exception as e:
        print(e)
