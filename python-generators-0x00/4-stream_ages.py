import mysql.connector
from mysql.connector import Error

def stream_user_ages():
    """Generator function to yield user ages one by one."""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your MySQL username
            password="qwertyuiop",  # Replace with your actual MySQL password
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT age FROM user_data")
            # Yield each age one at a time
            for row in cursor:
                yield float(row[0])  # Convert Decimal to float for easier computation
    except Error as e:
        print(f"Error streaming ages: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def calculate_average_age():
    """Calculate the average age using the stream_user_ages generator."""
    total_age = 0
    count = 0
    # Accumulate sum and count using the generator
    for age in stream_user_ages():
        total_age += age
        count += 1
    
    # Compute average, handle division by zero
    if count == 0:
        return 0
    return total_age / count

if __name__ == "__main__":
    # Calculate and print the average age
    average_age = calculate_average_age()
    print(f"Average age of users: {average_age:.2f}")