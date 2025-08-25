from kafka import KafkaConsumer
from configs import kafka_config
import json
from datetime import datetime

# Створення Kafka Consumer для читання сповіщень
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',  # Читаємо тільки нові сповіщення
    enable_auto_commit=True,
    group_id='iot_alerts_monitor'
)

# Назви топіків з сповіщеннями
my_name = "yosyp555"
temp_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Підписка на обидва топіки зі сповіщеннями
consumer.subscribe([temp_alerts_topic, humidity_alerts_topic])

print("Launching of IoT Alerts Monitoring System")
print(f"Temperature alerts monitoring: {temp_alerts_topic}")
print(f"Humidity alerts monitoring: {humidity_alerts_topic}")
print("Waiting for alerts...")
print("*" * 30)

# Лічильники сповіщень
total_alerts = 0
temp_alerts_count = 0
humidity_alerts_count = 0

try:
    for message in consumer:
        alert_data = message.value
        total_alerts += 1

        # Визначаємо тип сповіщення
        if message.topic == temp_alerts_topic:
            temp_alerts_count += 1
            alert_type = "TEMPERATURE"
        else:
            humidity_alerts_count += 1
            alert_type = "HUMIDITY"

        # Виводимо деталі сповіщення
        print(f"Sensor: {alert_data['sensor_id']}")
        print(f"Alert time: {alert_data['timestamp']}")

        # Специфічна інформація залежно від типу тривоги
        if 'temperature' in alert_data:
            print(f"Temperature: {alert_data['temperature']}°C")
            print(f"Threshold: {alert_data['threshold']}°C")

        if 'humidity' in alert_data:
            print(f"Humidity: {alert_data['humidity']}%")
            if alert_data['alert_type'] == 'HUMIDITY_HIGH':
                print(f"Max threshold: {alert_data['threshold_high']}%")
            elif alert_data['alert_type'] == 'HUMIDITY_LOW':
                print(f"Min threshold: {alert_data['threshold_low']}%")

        # Головне повідомлення
        print(f"Message: {alert_data['message']}")
        print("*" * 30)

except KeyboardInterrupt:
    print(f"\nStopping IoT Alerts Monitoring System")
    print(f"\tCompletely received: {total_alerts}")
    print(f"\tTemperature alerts: {temp_alerts_count}")
    print(f"\tHumidity alerts: {humidity_alerts_count}")
    print("*" * 30)
except Exception as e:
    print(f"Monitoring error: {e}")
finally:
    consumer.close()
    print("Consumer is closed")
