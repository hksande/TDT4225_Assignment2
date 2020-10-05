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

    def query6(self):
        query = "" 

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
