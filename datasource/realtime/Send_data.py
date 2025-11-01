# env_send.py
import json
import random
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from env_gen import generate_environment_data, create_device_user_mapping

KAFKA_TOPIC = "environment_iot_realtime"
KAFKA_SERVER = "localhost:9092"

random.seed(1510)

def convert_np(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Không thể chuyển kiểu: {type(obj)}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=convert_np).encode("utf-8")
)

# --- Input ---
num_devices = int(input("Nhập số lượng thiết bị: "))
num_users = int(input("Nhập số lượng người dùng: "))
date_str = input("Nhập ngày cần sinh dữ liệu (yyyy-mm-dd): ")

try:
    devices = create_device_user_mapping(num_devices, num_users)

    print("\n📌 Mapping User – Device (1–1):")
    for d in devices:
        print(f"  Device {d['device_id']} ↔ User {d['user_id']}")

    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_time = target_date
    end_time = target_date + timedelta(days=1)
    interval = timedelta(minutes=5)

    print(f"\n⏳ Bắt đầu sinh dữ liệu cho ngày {date_str}...\n")

    timestamp = start_time
    record_count = 0

    while timestamp < end_time:
        batch = generate_environment_data(devices, timestamp)

        for record in batch:
            producer.send(KAFKA_TOPIC, value=record)
            print("Sent:", record)
            record_count += 1

        producer.flush()
        timestamp += interval

    print(f"\n✅ Đã gửi {record_count} bản ghi đến Kafka topic '{KAFKA_TOPIC}'")

except ValueError as ve:
    print(str(ve))

finally:
    producer.flush()
