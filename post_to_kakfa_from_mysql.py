import util
import json
import datetime
import socket
from  confluent_kafka import Producer


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
def getProducer():
    conf = {'bootstrap.servers': "localhost:9092,localhost:9091",
            'client.id': socket.gethostname()}
    return Producer(conf)
def push_to_kafka(producer, topic, datalist, acked):
    for ticker in datalist:
        print(ticker)
        ticker_message = json.dumps(ticker, indent=2).encode('utf-8')
        producer.produce(topic, value=ticker_message)
        producer.flush()
def getJsonList(cursor,ticker_results):
    header = [i[0] for i in cursor.description]
    ticker_results_list = []
    for row in ticker_results:
        ticker = {}
        for prop, val in zip(header, row):
            ticker[prop] = val
        ticker_results_list.append(ticker)
    return json.dumps(ticker_results_list,default=json_serial)
def pushDataToEs(client, ticker_list):
    i=0
    for ticker in ticker_list:
        resp = client.index(index="events", id=i, document=ticker)
        print(resp['result'])
        i=i+1

if __name__ == '__main__':
    producer = getProducer()
    connection = util.geteSqlConnection('localhost','root','12345678','thales')
    cursor = connection.cursor()
    cursor.execute('select * from tickers limit 56')
    ticker_results = cursor.fetchall()
    client = util.getEsCLient()
    ticker_json_list = getJsonList(cursor, ticker_results)
    json_list = json.loads(ticker_json_list)
    push_to_kafka(producer,'events',json_list,acked)
   # pushDataToEs(client,json_list)




