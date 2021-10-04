import tweepy
import schedule
import os
import psycopg2

from dotenv import load_dotenv

load_dotenv()


def postStatus(maxSentence, minSentence):

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(os.getenv("API_KEY"), os.getenv("API_SECRET"))
    auth.set_access_token(os.getenv("ACCESS_TOKEN"),
                      os.getenv("ACCESS_TOKEN_SECRET"))

    # Create API object
    api = tweepy.API(auth, wait_on_rate_limit=True,
                    wait_on_rate_limit_notify=True)

    try:
        api.verify_credentials()
        print("Authentication OK")
        api.update_status(maxSentence)
        api.update_status(minSentence)
    except Exception as e:
        print("An error occured: " + str(e))


def printData():
    try:

        print("I'm working")
        DATABASE_URL = os.getenv('SB_CONN_STRING')

        con = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = con.cursor()

        cursor.execute("SELECT MAX(dates) FROM farmdates;")
        lastDate = cursor.fetchone()[0]
        formattedDate = lastDate.strftime("%b %d, %Y")
        print("Date ", lastDate)

        cursor.execute(
           f"SELECT high, commodity, parish, variety FROM farmdata1 WHERE high=(SELECT MAX(high) from farmdata1 WHERE date='{lastDate}') AND date='{lastDate}';")
        max_one = cursor.fetchone()

        cursor.execute(
           f"SELECT low, commodity, parish, variety FROM farmdata1 WHERE low=(SELECT MIN(low) from farmdata1 WHERE date='{lastDate}') AND date='{lastDate}';")
        min_one = cursor.fetchone()

        cursor.execute(
           f"SELECT high, commodity, parish, variety FROM farmdata2 WHERE high=(SELECT MAX(high) from farmdata2 WHERE date='{lastDate}') AND date='{lastDate}';")
        max_two = cursor.fetchone()

        cursor.execute(
           f"SELECT low, commodity, parish, variety FROM farmdata2 WHERE low=(SELECT MIN(low) from farmdata2 WHERE date='{lastDate}') AND date='{lastDate}';")
        min_two = cursor.fetchone()

        if (max_one[0] > max_two[0]):
            max = max_one
        else:
            max = max_two
        
        if (min_one[0] > min_two[0]):
            min = min_two
        else:
            min = min_one

        maxSentence = f"The {max[3]} variety of {max[1]} in {max[2]} is the most expensive commodity published in JAMIS' Week Ending - {formattedDate}' report, at ${max[0]} per kg."
        minSentence = f"The {min[3]} variety of {min[1]} in {min[2]} is the least expensive commodity published in JAMIS' 'Week Ending - {formattedDate}' report, at ${min[0]} per kg."
        
       
        postStatus(maxSentence=maxSentence, minSentence=minSentence)
        print("Max ", maxSentence)
        print("Min ", minSentence)

        cursor.close()
    except:
        print("Unexpected error:", sys.exc_info()[0])

# UTC Time -- 12:00 is the same as 7am local time
schedule.every().monday.at("12:00").do(printData)

while True:
    schedule.run_pending()

printData()
