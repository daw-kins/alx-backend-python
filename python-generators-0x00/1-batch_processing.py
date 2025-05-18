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
            user="root",  # Replace with your MySQL username
            password="qwetyuiop",  # Replace with your actual MySQL password
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT user_id, name, email, age FROM user_data")
            
            # Check if there are any rows at all
            cursor.execute("SELECT COUNT(*) FROM user_data")
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                return  # Explicit return for empty table
            
            # Re-execute the query to fetch rows
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
    # Flag to track if any users are yielded
    has_yielded = False
    # Iterate over batches from stream_users_in_batches
    for batch in stream_users_in_batches(batch_size):
        # Filter users over 25 years old in the batch
        for user in batch:
            if user[3] > 25:  # user[3] is the age column (Decimal type)
                yield user
                has_yielded = True
    # Return if no users were yielded (edge case)
    if not has_yielded:
        return  # Explicit return for no filtered users

if __name__ == "__main__":
    # Example usage: Process users in batches of 2
    batch_size = 2
    print(f"Processing users in batches of {batch_size}, filtering for age > 25:")
    for user in batch_processing(batch_size):
        print(f"Filtered User: {user}")