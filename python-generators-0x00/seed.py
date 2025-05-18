import mysql.connector
import csv
import uuid
from mysql.connector import Error

print("Starting the script...")

def connect_db():
    """Connects to the MySQL database server."""
    print("Attempting to connect to MySQL server...")
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  
            password="qwertyuiop"  
        )
        if connection.is_connected():
            print("Successfully connected to MySQL server")
        else:
            print("Connection established but not active")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL server: {e}")
        return None

def create_database(connection):
    """Creates the database ALX_prodev if it does not exist."""
    print("Creating database ALX_prodev...")
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev created or already exists")
    except Error as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()

def connect_to_prodev():
    """Connects to the ALX_prodev database in MySQL."""
    print("Attempting to connect to ALX_prodev database...")
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  
            password="qwertyuiop",  
            database="ALX_prodev"
        )
        if connection.is_connected():
            print("Successfully connected to ALX_prodev database")
        else:
            print("Connection established but not active")
        return connection
    except Error as e:
        print(f"Error connecting to ALX_prodev database: {e}")
        return None

def create_table(connection):
    """Creates a table user_data if it does not exist with the required fields."""
    print("Creating table user_data...")
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(5,2) NOT NULL,
            INDEX idx_user_id (user_id)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table user_data created or already exists")
    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

def insert_data(connection, data):
    """Inserts data into the database if it does not exist."""
    print("Inserting data into user_data table...")
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT IGNORE INTO user_data (user_id, name, email, age)
        VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"{cursor.rowcount} rows inserted successfully")
    except Error as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()

def main():
    print("Entering main function...")
    
    # Step 1: Connect to MySQL server
    connection = connect_db()
    if not connection:
        print("Failed to connect to MySQL server, exiting...")
        return

    # Step 2: Create the ALX_prodev database
    create_database(connection)
    connection.close()
    print("Closed connection to MySQL server")

    # Step 3: Connect to ALX_prodev database
    connection = connect_to_prodev()
    if not connection:
        print("Failed to connect to ALX_prodev database, exiting...")
        return

    # Step 4: Create the user_data table
    create_table(connection)

    # Step 5: Read CSV file and insert data
    print("Attempting to read user_data.csv...")
    try:
        with open("user_data.csv", "r") as file:
            print("Successfully opened user_data.csv")
            csv_reader = csv.reader(file)
            header = next(csv_reader)  
            print(f"CSV header: {header}")
            data = []
            for row in csv_reader:
                print(f"Processing row: {row}")
                user_id = row[0] if row[0] else str(uuid.uuid4())  # Ensure valid UUID
                data.append((user_id, row[1], row[2], float(row[3])))
            insert_data(connection, data)
    except FileNotFoundError:
        print("user_data.csv file not found")
    except Exception as e:
        print(f"Error reading CSV or inserting data: {e}")
    finally:
        connection.close()
        print("Closed connection to ALX_prodev database")

if __name__ == "__main__":
    print("Script execution started")
    main()
    print("Script execution finished")