# env_send.py
import json
import random
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from Gen_data import generate_environment_data, create_device_user_mapping

KAFKA_TOPIC = "home_sensor"
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
datetime_str = input("Nhập thời điểm bắt đầu sinh dữ liệu (yyyy-mm-dd HH:MM): ")

try:
    devices = create_device_user_mapping(num_devices, num_users)

    print("\n📌 Mapping User – Device (1–1):")
    for d in devices:
        print(f"  Device {d['device_id']} ↔ User {d['user_id']}")

    # ✅ Parse datetime thay vì chỉ date
    start_time = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    end_time = start_time + timedelta(days=1)   # sinh 24 giờ kể từ thời điểm nhập
    interval = timedelta(minutes=5)

    print(f"\n⏳ Bắt đầu sinh dữ liệu từ {start_time} tới {end_time}...\n")

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

    print(f"\n✅ Đã gửi {record_count} bản ghi tới Kafka topic '{KAFKA_TOPIC}'")

except ValueError as ve:
    print("❌ Lỗi:", ve)

finally:
    producer.flush()
