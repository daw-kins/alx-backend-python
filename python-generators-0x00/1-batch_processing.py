import mysql.connector
from mysql.connector import Error

def stream_users_in_batches(batch_size):
    """Generator function to fetch rows from user_data table in batches."""
    connection = None
    cursor = None
    try:
        # Connect to the ALX_prodev database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  
            password="qwertyuiop",  
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT user_id, name, email, age FROM user_data")
            
            # Fetch rows in batches using cursor.fetchmany()
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:  # Break if no more rows to fetch
                    break
                yield batch  # Yield the batch as a list of rows
    except Error as e:
        print(f"Error fetching users in batches: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def batch_processing(batch_size):
    """Generator function to process batches and filter users over 25 years old."""
    # Iterate over batches from stream_users_in_batches
    for batch in stream_users_in_batches(batch_size):
        # Filter users over 25 years old in the batch
        for user in batch:
            if user[3] > 25:  # user[3] is the age column (Decimal type)
                yield user

if __name__ == "__main__":
    # Example usage: Process users in batches of 2
    batch_size = 2
    print(f"Processing users in batches of {batch_size}, filtering for age > 25:")
    for user in batch_processing(batch_size):
        print(f"Filtered User: {user}")