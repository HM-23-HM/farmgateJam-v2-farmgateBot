import tweepy
import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='farm',
                                         user='root',
                                         password='')
    if connection.is_connected():

        cursor = connection.cursor()
        cursor.execute("SELECT MAX(date) FROM `datestable`;")
        lastDate = cursor.fetchone()[0]
        formattedDate = lastDate.strftime("%b %d, %Y")
        print("Date ", lastDate)
    
        cursor.execute(f'SELECT high, commodity, parish, variety FROM `farmtable` WHERE high=(SELECT MAX(high) from `farmtable` WHERE date="{lastDate}") AND date="{lastDate}";')
        max = cursor.fetchone()

        cursor.execute(f'SELECT low, commodity, parish, variety FROM `farmtable`  WHERE low=(SELECT MIN(low) from `farmtable` WHERE date="{lastDate}") AND date="{lastDate}";')
        min = cursor.fetchone()

        maxSentence = f"The {max[3]} variety of {max[1]} in {max[2]} is the most expensive commodity published in JAMIS' 'Week Ending - {formattedDate}' report, at ${max[0]} per kg."
        minSentence = f"The {min[3]} variety of {min[1]} in {min[2]} is the least expensive commodity published in JAMIS' 'Week Ending - {formattedDate}' report, at ${min[0]} per kg."

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")



# # Authenticate to Twitter
# auth = tweepy.OAuthHandler("nDdKRZcNZqoBgeYPxpDulK8N7", 
#     "L4LfXFTe7wqILZg9p1EMUFxyBIM1sQ5ukWvn4iJbU1n7AMKVJq")
# auth.set_access_token("1432137371642912770-27xhfrhr5LjI8ZnItqYUEL5frfWnYo", 
#     "7AJxjyXn13dcZzIbCSdMeR4RXAJeCKxRHXHG7u0mKc4Z4")

# # Create API object
# api = tweepy.API(auth, wait_on_rate_limit=True,
#     wait_on_rate_limit_notify=True)

# try:
#     api.verify_credentials()
#     print("Authentication OK")
#     api.update_status(maxSentence)
#     api.update_status(minSentence)
# except Exception as e:
#     print("An error occured: " + str(e))