import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os


class LearnPostgresql:
    """
    Learn postgresql basics - CRED operations
    """
    def __init__(self):
        self.db_name = os.getenv("database")
        self.port = 5432
        self.host = os.getenv("host")
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.sql_instance = None
        self.create_connection_sql()

    def create_connection_sql(self):
        try:
            self.sql_instance = psycopg2.connect(
                port=5432,
                host=self.host,
                database=self.db_name,
                user=self.user,
                password=self.password,
            )
            print("successfully connected")
            print(self.sql_instance)

        except Exception as e:
            print("Error while connecting :")
            print(e)

    def create_table_into_sql(self):
        # Open a cursor to perform database operations
        cur = self.sql_instance.cursor()
        # Create a table called Students
        cur.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    student_id SERIAL PRIMARY KEY,
                    student_name VARCHAR (100) NOT NULL,
                    class VARCHAR (20),
                    blood_group VARCHAR (10),
                    contact BIGINT NOT NULL,
                    address VARCHAR (200) NOT NULL
                )
                """)
        self.sql_instance.commit()
        cur.close()
        self.sql_instance.close()
        print("Table Created Successfully :")

    def insert_data_into_table(self):
        query = """INSERT INTO students (student_id, student_name, class, blood_group, contact, address) VALUES (%s, %s, %s, %s, %s, %s);"""
        data = [
            (2, 'Ravi', '9th', 'O+ve', '9876543210', 'Chennai'),
            (3, 'Priya', '11th', 'A+ve', '8123456789', 'Bangalore'),
            (4, 'Deepa', '10th', 'B-ve', '9876567890', 'Hyderabad'),
            (5, 'Manoj', '12th', 'AB+ve', '9012345678', 'Coimbatore')
            ]
        cur = self.sql_instance.cursor()
        try:
            # if insert single data use execute() function
            cur.executemany(query, data)
            self.sql_instance.commit()
            cur.close()
            self.sql_instance.close()
            print(f"{cur.rowcount} Data inserted Successfully !")
        except Exception as e:
            print("Data inserted Failed !")
            print("Error", repr(e))

    def update_single_data(self):
        query = """UPDATE students SET student_name=%s WHERE student_id=%s"""
        updated_data = ("arun", 1)
        cur = self.sql_instance.cursor()
        cur.execute(query, updated_data)
        self.sql_instance.commit()
        cur.close()
        self.sql_instance.close()
        print("Update is done")

    def roll_back_in_sql(self):
        """
        If any error during transaction it will remain the data base in old state.
        """
        query_insert = """INSERT INTO students (student_id, student_name, class, blood_group, contact, address) VALUES (%s, %s, %s, %s, %s, %s);"""
        inserted_data = (8, "lokesh", "12th", "b+ve", "9675674533", "Salem")
        query_update = """UPDATE wrong_table SET address=%s WHERE student_id=%s"""
        updated_data = ("pune", 2)
        cur = self.sql_instance.cursor()
        try:
            cur.execute(query_insert, inserted_data)
            cur.execute(query_update, updated_data)
            self.sql_instance.commit()
            print("Transaction committed successfully!")
            print("Insert and Update both are committed Successfully !")
        except Exception as e:
            print("Error during multiple action check update or insert")
            print(repr(e))
            self.sql_instance.rollback() #undo all changes
        finally:
            cur.close()
            self.sql_instance.close()

    def read_all_data(self):
        query = """SELECT * FROM students"""
        cur = self.sql_instance.cursor()
        cur.execute(query)
        # fetch all - use in smaller data set
        # all_rows = cur.fetchall()
        # for row in all_rows:
        #     print(row)

        # # fetch one - memory reduced & used by large data set
        # row = cur.fetchone() # fetching first row
        # while row:
        #     print(row)
        #     row = cur.fetchone() # fetch next now

        # fetch many
        count = 0
        while True:
            row = cur.fetchmany(size=2)  # fetching first row
            count += 1
            print(f"count value is : {count}")
            if not row:
                break
            for r in row:
                print(r)


sql = LearnPostgresql()
sql.read_all_data()