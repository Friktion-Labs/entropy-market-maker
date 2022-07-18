import requests
from discord import Webhook, RequestsWebhookAdapter
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

url = "https://discord.com/api/webhooks/998686560495095848/kHPDCW_ecMhqlraKcBeCwdxhrUQ9u5m8NEpgAVrLfyIoyhov8A5dX_JNcyCsjNj5MRd2"
webhook = Webhook.from_url(url, adapter=RequestsWebhookAdapter())
webhook.send("Hello World")

db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

stale_time = 86400
max_leverage = 200
channelid = 997266071167971372
table = 'mm_data'

dbstring = (
    f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

sqlconn = create_engine(dbstring).connect()

mmData = pd.read_sql_table(table, sqlconn)
df = mmData

markets = df.marketName.unique()
for market in markets:
    lastData = np.array(df[df.marketName == market]
                        ['time'].sort_values(ascending=False))[0]
    lastPrice = np.array(
        df[df.marketName == market]['oraclePrice'].sort_values(ascending=False))[0]
    lastDataTime = lastData.timestamp()
    now = pd.to_datetime('now').timestamp()
    timesincelastdata = now-lastDataTime
    print(f'info for: {market}')
    print(f'last data: {lastData}')
    print(f'now: {pd.to_datetime("now")}')
    print(f'hours since last data: {(timesincelastdata)/3600}')
    if timesincelastdata > stale_time:
        webhook.send(
            f'stale data warning for {market}: hours since last data: {(timesincelastdata)/3600}')
    else:
        webhook.send(f'{market} data up to date!')
    lastBasePos = np.array(
        df[df.marketName == market]['basePos'].sort_values(ascending=False))[0]
    lastNotionalDelta = lastBasePos * lastPrice
    lastLeverage = np.array(
        df[df.marketName == market]['leverage'].sort_values(ascending=False))[0]
    print(f'last notional delta: {lastNotionalDelta}')
    print(f'last leverage: {lastLeverage}%')
    if abs(lastLeverage) > max_leverage:
        webhook.send(
            f'leverage warning for {market}: current leverage: {lastLeverage}%')
    else:
        webhook.send(f'{market} leverage is within bounds!')
    print('='*35)
