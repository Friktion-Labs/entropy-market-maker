import requests
from discord import Webhook, RequestsWebhookAdapter
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import asyncio
import traceback
load_dotenv()

# channel url
url = "https://discord.com/api/webhooks/998686560495095848/kHPDCW_ecMhqlraKcBeCwdxhrUQ9u5m8NEpgAVrLfyIoyhov8A5dX_JNcyCsjNj5MRd2"
# set up webhook using the url
webhook = Webhook.from_url(url, adapter=RequestsWebhookAdapter())
# send test message to url
webhook.send("Hello World")

# connection credentials
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# connection
try:
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass,
    )
    cur = conn.cursor()
except Exception as e:
    print("ERROR: Unable to connect to database:", traceback.print_exc())

# check data


async def checkStaleData(timeout=60, staletime=86400, market='BTC-PERP'):
    while True:
        # create sql query
        def query():
            return f"""
            SELECT "time", "marketName" FROM public.mm_data
            WHERE "marketName" = '{market}'
            ORDER BY id DESC LIMIT 1
            """
        # run sql query
        cur.execute(query())

        # grab timestamp
        rows = cur.fetchall()
        time = rows[0][0]
        lastTimeStamp = time.timestamp()

        # find current timestamp
        now = pd.to_datetime('now').timestamp()

        # compare
        timesincelastdata = now-lastTimeStamp

        # send discord message using webhook based on result
        if timesincelastdata > staletime:
            webhook.send(
                f'{market}: hours since last data: {(timesincelastdata)/3600}')
        else:
            webhook.send(f'{market}: up to date!')

        await asyncio.sleep(timeout)

# call function for each market


async def main():
    await asyncio.gather(checkStaleData(timeout=60, staletime=86400, market='BTC-PERP'), checkStaleData(timeout=60, staletime=86400, market='BTC^2-PERP'), checkStaleData(timeout=60, staletime=86400, market='BTC_1D_IV-PERP'))

# helps export function to be run
if __name__ == "__main__":
    asyncio.run(main())


conn.close()
