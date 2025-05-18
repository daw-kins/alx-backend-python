import mysql.connector
from mysql.connector import Error

def stream_users():
    """Generator function to stream rows from the user_data table one by one."""
    connection = None
    cursor = None
    try:
        # Establish connection to the ALX_prodev database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  
            password="qwertyuiop",  
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            # Execute query to select all rows from user_data
            cursor.execute("SELECT user_id, name, email, age FROM user_data")
            # Yield each row one at a time
            for row in cursor:
                yield row
    except Error as e:
        print(f"Error streaming users: {e}")
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    # Example usage of the generator
    for user in stream_users():
        print(f"User: {user}")

