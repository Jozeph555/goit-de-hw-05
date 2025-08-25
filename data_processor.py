from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
from datetime import datetime

# Створення Kafka Consumer для читання даних датчиків
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',  # Читаємо тільки нові повідомлення
    enable_auto_commit=True,
    group_id='iot_data_processor'
)

# Створення Kafka Producer для відправки сповіщень
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назви топіків
my_name = "yosyp555"
input_topic = f'{my_name}_building_sensors'
temp_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Пороги для тривоги
TEMP_THRESHOLD = 40.0  # Температура вище 40°C
HUMIDITY_HIGH = 80.0  # Вологість вище 80%
HUMIDITY_LOW = 20.0  # Вологість нижче 20%

# Підписка на топік з даними датчиків
consumer.subscribe([input_topic])

print("Launching of IoT data processing system")
print(f"\tReading from the topic: {input_topic}")
print(f"\tSending temperature alerts to: {temp_alerts_topic}")
print(f"\tSending humidity alerts to: {humidity_alerts_topic}")
print("*" * 30)

processed_messages = 0
temperature_alerts = 0
humidity_alerts = 0

try:
    for message in consumer:
        sensor_data = message.value
        processed_messages += 1

        print(f"Message processing: {processed_messages}")
        print(f"\tSensor: {sensor_data['sensor_id']}")
        print(f"\tTemperature: {sensor_data['temperature']}°C")
        print(f"\tHumidity: {sensor_data['humidity']}%")

        # Перевірка температури
        if sensor_data['temperature'] > TEMP_THRESHOLD:
            temperature_alert = {
                "sensor_id": sensor_data['sensor_id'],
                "alert_type": "TEMPERATURE_HIGH",
                "temperature": sensor_data['temperature'],
                "threshold": TEMP_THRESHOLD,
                "timestamp": sensor_data['timestamp'],
                "message": f"CRITICAL_TEMPERATURE! {sensor_data['temperature']}°C exceeds the threshold {TEMP_THRESHOLD}°C"
            }

            # Відправляємо температурне сповіщення
            producer.send(
                temp_alerts_topic,
                key=sensor_data['sensor_id'],
                value=temperature_alert
            )
            producer.flush()

            temperature_alerts += 1
            print(f"TEMPERATURE ALERT! Alert sent")

        # Перевірка вологості
        humidity = sensor_data['humidity']
        humidity_alert_sent = False

        if humidity > HUMIDITY_HIGH:
            humidity_alert = {
                "sensor_id": sensor_data['sensor_id'],
                "alert_type": "HUMIDITY_HIGH",
                "humidity": humidity,
                "threshold_high": HUMIDITY_HIGH,
                "timestamp": sensor_data['timestamp'],
                "message": f"HIGH HUMIDITY! {humidity}% exceeds the threshold {HUMIDITY_HIGH}%"
            }
            humidity_alert_sent = True

        elif humidity < HUMIDITY_LOW:
            humidity_alert = {
                "sensor_id": sensor_data['sensor_id'],
                "alert_type": "HUMIDITY_LOW",
                "humidity": humidity,
                "threshold_low": HUMIDITY_LOW,
                "timestamp": sensor_data['timestamp'],
                "message": f"LOW HUMIDITY! {humidity}% bolow the threshold {HUMIDITY_LOW}%"
            }
            humidity_alert_sent = True

        # Відправляємо сповіщення про вологість
        if humidity_alert_sent:
            producer.send(
                humidity_alerts_topic,
                key=sensor_data['sensor_id'],
                value=humidity_alert
            )
            producer.flush()

            humidity_alerts += 1
            print(f"HUMIDITY ALERT! Alert sent")

        print('*' * 30)

except KeyboardInterrupt:
    print(f"\nStopping IoT data processing system")
    print(f"\tComletely processed: {processed_messages}")
    print(f"\tTemperature alerts: {temperature_alerts}")
    print(f"\tHumidity alerts: {humidity_alerts}")
except Exception as e:
    print(f"Processing error: {e}")
finally:
    consumer.close()
    producer.close()
    print("Consumer and Producer are closed")