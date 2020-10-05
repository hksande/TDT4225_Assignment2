from DbConnector import DbConnector

class Tasks:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def query1(self, table_name):
        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def query2(self):
        query = """SELECT activities.count / users.count 
            FROM
            (SELECT COUNT(*) AS count FROM Activity) AS activities, 
            (SELECT COUNT(*) AS count FROM User) AS users"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def query3(self):
        query = """SELECT user_id, COUNT(id) AS the_count 
            FROM Activity 
            GROUP BY user_id
            ORDER BY the_count DESC
            LIMIT 20"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def query4(self):
        query = """SELECT DISTINCT user_id FROM ACTIVITY 
            WHERE transportation_mode = 'taxi'"""
        self.cursor.execute(query)
        self.db_connection.commit()

def main():
    tasks = Tasks()
    tasks.query1(User) #svaret blir 182
    tasks.query1(Activity) #svaret blir 16 048
    tasks.query1(TrackPoint) #svaret blir 9 681 756
    tasks.query2() #svaret blir 88.1758
    tasks.query3()
    tasks.query4()#får ut 20 user_id

if __name__ == '__main__':
    main()
