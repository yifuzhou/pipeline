import argparse
import requests
import atexit
import json
import logging
import schedule
import time


from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

API_BASE = 'https://api.gdax.com'

def check_symbol(symbol):
    """
    helper method if the symbol exists in coinbase API
    """
    logger.debug('Checking symbol.')
    try:
        response = requests.get(API_BASE + '/products')
        products_ids = [product['id'] for product in response.json()]

        if symbol not in products_ids:
            logger.warn("symbol %s not supported. The list of supported symbol: %s", symbol, products_ids)
            exit()
    except Exception as e:
            logger.warn('Failed to fetch products. caused by %s', e)

def fetch_price(symbol, producer, topic_name):
    """
    Helper function to retrieve data and send it to kafka
    """
    logger.debug('Start to fetch price for %s', symbol)
    try:
        response = requests.get('%s/products/%s/ticker' % (API_BASE, symbol))
        price = response.json()['price']

        timestamp = time.time()
        payload = {'Symbol':str(symbol),
                   'LastTradePrice':str(price),
                   'Timestamp':str(timestamp)}
        logger.debug('Retrieved %s info %s', symbol, payload)

        producer.send(topic=topic_name, value=json.dumps(payload), timestamp_ms=int(time.time()*1000))

        logger.debug(' Sent price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn( 'Failed to send messages to kafka, caused by: %s', timeout_error.message)
    except Exception as e:
        logger.warn(' Failed to fetch price: %s', e)

def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by %s', e.message)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help=' the symbol you want to pull.')
    parser.add_argument('topic_name', help=' the kafka topic push to.')
    parser.add_argument('kafka_broker', help=' the location fo the kafka broker.')

    # Parse arguments
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # Check if the symbol is supported.
    check_symbol(symbol)

    # Instantiate a simple kafka producer.
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    #Schedule and run the fetch_price function every second
    schedule.every(1).second.do(fetch_price, symbol, producer, topic_name)

    # Setup proper shutdown hook
    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)
