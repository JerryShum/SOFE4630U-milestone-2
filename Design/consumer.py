import mysql.connector

# configuration
db_config = {
    'user': 'usr',
    'password': 'sofe4630u',
    'host': '34.130.6.134',
    'database': 'Readings'
}

try:
    # connect to the "Middle Stage" storage
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # query the data that was "sinked" from Pub/Sub --> the data is in the SQL table (smartMeter)
    query = "SELECT * FROM SmartMeter ORDER BY time DESC"
    cursor.execute(query)

    print("--- Consumer Reading from Middle Storage ---")
    for (id, time, profile_name, temp, hum, press) in cursor:
        print(f"ID: {id} | Profile: {profile_name} | Temp: {temp}°C")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()