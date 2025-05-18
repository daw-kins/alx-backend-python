import mysql.connector
from mysql.connector import Error

def stream_users_in_batches(batch_size):
    """Generator function to fetch rows from user_data table in batches."""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="MySQLRoot123!",
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT user_id, name, email, age FROM user_data")
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                yield batch
    except Error as e:
        print(f"Error fetching users in batches: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def batch_processing(batch_size):
    """Collect and return filtered users over 25 years old."""
    filtered_users = []
    for batch in stream_users_in_batches(batch_size):
        for user in batch:
            if user[3] > 25:
                filtered_users.append(user)
    return filtered_users  # Explicit return of the list

if __name__ == "__main__":
    batch_size = 2
    print(f"Processing users in batches of {batch_size}, filtering for age > 25:")
    result = batch_processing(batch_size)
    for user in result:
        print(f"Filtered User: {user}")