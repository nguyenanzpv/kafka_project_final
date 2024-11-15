import configparser
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import time
import threading
import psycopg2

# Đọc cấu hình từ file kafka_config.ini
config = configparser.ConfigParser()
config.read('kafka_config.ini')

# Đường dẫn tệp JSON chứa offset đã xử lý
processed_offsets_file = 'processed_offset.json'

# Lấy cấu hình Kafka từ phần [KAFKA] của file .ini và chuyển thành dictionary
kafka_config = {key: value for key, value in config.items('KAFKA')}
topic = kafka_config.pop('topic')  # Lấy và xóa 'topic' ra khỏi kafka_config để dùng riêng biệt

# Lấy cấu hình Kafka từ phần [KAFKA-LOCAL] của file .ini và chuyển thành dictionary
kafka_config_local = {key: value for key, value in config.items('KAFKA-LOCAL')}
topic_local = kafka_config_local.pop('topic')


# PostgreSQL connection
def get_database_config():
    # Lấy các giá trị từ section [postgresql]
    db_params = {
        'host': config['postgresql']['host'],
        'port': config['postgresql']['port'],
        'database': config['postgresql']['database'],
        'user': config['postgresql']['user'],
        'password': config['postgresql']['password']
    }
    return db_params


# Ghi và đọc offset đã xử lý
def read_processed_offsets():
    try:
        with open(processed_offsets_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_processed_offsets(processed_offsets):
    with open(processed_offsets_file, 'w') as f:
        json.dump(processed_offsets, f, indent=4)


def clean_old_offsets(processed_offsets, days=1):
    threshold_time = time.time() - days * 24 * 60 * 60  #xoa sau 1 ngay
    return {k: v for k, v in processed_offsets.items() if v > threshold_time}


# PostgreSQL insert
def insert_db(message_json):
    db_params = get_database_config()
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO kafka_consume (_id, time_stamp, ip, user_agent, resolution, user_id_db, device_id, api_version, store_id, local_time, show_recommendation, current_url, referrer_url, email_address, collection, option)
                    VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    message_json.get('_id'),
                    message_json.get('time_stamp'),
                    message_json.get('ip'),
                    message_json.get('user_agent'),
                    message_json.get('resolution'),
                    message_json.get('user_id_db'),
                    message_json.get('device_id'),
                    message_json.get('api_version'),
                    message_json.get('store_id'),
                    message_json.get('local_time'),
                    message_json.get('show_recommendation'),
                    message_json.get('current_url'),
                    message_json.get('referrer_url'),
                    message_json.get('email_address'),
                    message_json.get('collection'),
                    json.dumps(message_json.get('option'))
                ))
                conn.commit()
    except Exception as e:
        print("Error inserting to PostgreSQL:", e)


# Kafka consumer handler class
class KafkaConsumerHandler:
    def __init__(self, config, topic, processed_file):
        self.config = config
        self.topic = topic
        self.processed_file = processed_file
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
        self.processed_offset = clean_old_offsets(read_processed_offsets())
        write_processed_offsets(self.processed_offset)

    def process_message(self, message):
        raise NotImplementedError("Ko co subclass nao dang trien khai class nay")

    def run(self):
        try:
            print(f"Listening to topic: {self.topic}")
            while True:
                msg = self.consumer.poll(1.0)  # Chờ 1 giây cho mỗi lần kiểm tra
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                partition = msg.partition()
                offset = msg.offset()

                if self.processed_offset.get(partition - 1) >= offset:
                    print(f"Duplicate message with offset: {offset} in partition {partition}. Skipping.")
                    continue

                #gan offset moi  vao file
                self.processed_offset[partition] = offset
                write_processed_offsets(self.processed_offset)

                #xu ly message
                message_value = msg.value().decode('utf-8')
                message_json = json.loads(message_value)

                self.process_message(message_json)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


# Kafka handler for processing to local Kafka
class LocalKafkaProduceHandler(KafkaConsumerHandler):
    def __init__(self, config, topic, processed_file, config_local):
        super().__init__(config, topic, processed_file)
        self.local_producer = Producer(config_local)

    def process_message(self, message_json):
        print(f"Producing to local Kafka: {message_json}")
        self.local_producer.produce(topic_local, key=None, value=json.dumps(message_json))


# Kafka handler for processing to PostgreSQL
class PostgresHandler(KafkaConsumerHandler):
    def process_message(self, message_json):
        print(f"Inserting to PostgreSQL: {message_json}")
        insert_db(message_json)


#main function
if __name__ == '__main__':
    local_producer_handler = LocalKafkaProduceHandler(kafka_config, topic, processed_offsets_file, kafka_config_local)
    postgres_handler = PostgresHandler(kafka_config_local, topic_local, processed_offsets_file)

    #tao thread chay task
    threading1 = threading.Thread(target=local_producer_handler.run())
    threading2 = threading.Thread(target=postgres_handler.run())

    #start thread
    threading1.start()
    threading2.start()

    #join thread
    threading1.join()
    threading2.join()
