import requests
import datetime
import sqlalchemy
from sqlalchemy import create_engine

user = 'postgres'
database = 'crypto'
host = 'localhost'

# url for database
db_url = str('postgres://' + user + '@' + host + '/' + database)

eng = create_engine(db_url)

meta = sqlalchemy.MetaData(bind=eng, reflect=True)
wtc_Table = meta.tables['wtc_data']



response = requests.get('https://min-api.cryptocompare.com/data/histohour?fsym=WTC&tsym=USD&e=CCCAGG')

coin_json = response.json()['Data']

print(len(coin_json))

times = []

for entry in coin_json:
    times.append(entry['time'])
    try:
        insert = wtc_Table.insert().values(
                open=entry['open'],
                low=entry['low'],
                high=entry['high'],
                close=entry['close'],
                time=entry['time']
        )
        eng.execute(insert)
    except Exception as e:
        print(e)
        continue

print(str(max(times)))
print(str(min(times)))
