import configparser
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
import psycopg2
import threading

# Đọc cấu hình từ file kafka_config.ini
config = configparser.ConfigParser()
config.read('kafka_config.ini')
# Đường dẫn tệp JSON chứa _id đã xử lý
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


def insert_db(message_json):
    db_params = get_database_config()
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Kết nối thành công với PostgreSQL")

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
            json.dumps(message_json.get('option'))  # Chuyển đổi dictionary option thành JSON
        ))

        conn.commit()

    except Exception as e:
        print("Không thể kết nối với PostgreSQL:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Đóng kết nối")


# Hàm đọc offset đã xử lý từ tệp JSON
def read_processed_offsets():
    try:
        with open(processed_offsets_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# Hàm ghi offset đã xử lý vào tệp JSON
def write_processed_offsets(processed_offsets):
    with open(processed_offsets_file, 'w') as f:
        json.dump(processed_offsets, f, indent=4)


# Hàm xóa offset đã quá 7 ngày
def clean_old_offsets(processed_offsets):
    current_time = time.time()
    threshold_time = current_time - 1 * 24 * 60 * 60  # 1 ngày tính theo giây
    cleaned_offsets = {k: v for k, v in processed_offsets.items() if v > threshold_time}
    return cleaned_offsets


def get_topic(kafka_config, kafka_config_local):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    processed_offsets = read_processed_offsets()  # Đọc offset đã xử lý từ tệp JSON
    processed_offsets = clean_old_offsets(processed_offsets)  # Xóa các offset đã quá hạn
    write_processed_offsets(processed_offsets)  # Lưu lại danh sách offset đã được làm sạch
    try:
        print(f"Listening to topic: {topic}")
        while True:
            msg = consumer.poll(1.0)  # Chờ 1 giây cho mỗi lần kiểm tra
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            print(f"Key: {msg.key()}, Value: {msg.value()}")

            # Lấy partition và offset của msg hiện tại
            partition = msg.partition()
            offset = msg.offset()

            # Kiểm tra nếu offset đã được xử lý
            if processed_offsets.get(partition, -1) >= offset:
                print(f"Duplicate message with offset: {offset} in partition {partition}. Skipping.")
                continue

            # Nếu chưa xử lý, cập nhật offset vào dictionary và xử lý message
            processed_offsets[partition] = offset
            write_processed_offsets(processed_offsets)  # Cập nhật tệp JSON

            # Giải mã message value từ byte và chuyển thành JSON
            message_value = msg.value().decode('utf-8')
            message_json = json.loads(message_value)

            # Produce message đến kafka local
            produce_topic_local(kafka_config_local, None, msg.value())

            # Xử lý message
            print(f"Processing message with offset: {offset} in partition {partition}")
            print(f"Message: {message_json}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def get_topic_local(kafka_config_local):
    consumer = Consumer(kafka_config_local)
    consumer.subscribe([topic_local])

    try:
        print(f"Listening to topic: {topic}")
        while True:
            msg = consumer.poll(1.0)  # Chờ 1 giây cho mỗi lần kiểm tra
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            print(f"Key: {msg.key()}, Value: {msg.value()}")

            # Lấy partition và offset của thông điệp hiện tại
            partition = msg.partition()
            offset = msg.offset()

            # Giải mã message value từ byte và chuyển thành JSON
            message_value = msg.value().decode('utf-8')
            message_json = json.loads(message_value)

            # Produce message đến kafka local
            insert_db(message_json)

            # Xử lý message
            print(f"Processing message with offset: {offset} in partition {partition}")
            print(f"Message: {message_json}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def produce_topic_local(kafka_config_local, _id, msg):
    producer = Producer(kafka_config_local)
    try:
        topic = topic_local
        producer.produce(topic, key=_id, value=msg)
        producer.flush()
        print(f"Message sent to local Kafka: {msg}")
    except:
        raise


if __name__ == '__main__':
    # Tạo hai thread để chạy song song
    thread1 = threading.Thread(target=get_topic, args=(kafka_config, kafka_config_local))
    thread2 = threading.Thread(target=get_topic_local, args=(kafka_config_local,))

    # Khởi động các thread
    thread1.start()
    thread2.start()

    # Đợi các thread hoàn thành
    thread1.join()
    thread2.join()
