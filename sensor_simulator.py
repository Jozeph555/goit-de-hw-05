from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
from datetime import datetime

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "yosyp555"
topic_name = f'{my_name}_building_sensors'

# Генерування унікального ID датчика для запуску
sensor_id = f'sensor_{str(uuid.uuid4())}'
print(f'Starting the sensor: {sensor_id}')
print(f'Sending data to the topic: {topic_name}')
print('*' * 30)

# Імітація роботи датчика
try:
    message_count = 0
    while True:
        # Генеруємо випадкові показники
        temperature = round(random.uniform(25.0, 45.0), 2)  # 25-45°C
        humidity = round(random.uniform(15.0, 85.0), 2)  # 15-85%

        # Створюємо дані датчика
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": temperature,
            "humidity": humidity
        }

        # Відправляємо дані
        producer.send(
            topic_name,
            key=sensor_id,
            value=sensor_data
        )
        producer.flush()

        message_count += 1

        # Виводимо інформацію
        print(f"Message #{message_count}")
        print(f"\tTemperature: {temperature}°C")
        print(f"\tHumidity: {humidity}%")
        print(f"\tTime: {sensor_data['timestamp']}")

        # Індикатори оповіщеня
        alerts = []
        if temperature > 40:
            alerts.append("HIGH TEMPERATURE!")
        if humidity > 80:
            alerts.append("HIGH HUMIDITY!")
        elif humidity < 20:
            alerts.append("LOW HUMIDITY!")

        if alerts:
            print(f"Warning: {', '.join(alerts)}")

        print("-" * 30)

        # Затримка між вимірюваннями (3 секунди)
        time.sleep(3)

except KeyboardInterrupt:
    print(f"\nStopping the sensor: {sensor_id}")
    print(f"Messages sent: {message_count}")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
    print("Producer is closed")
