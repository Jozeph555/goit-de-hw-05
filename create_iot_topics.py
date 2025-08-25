from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення топіків для IoT системи
my_name = "yosyp555"
topics_to_create = [
    f'{my_name}_building_sensors',
    f'{my_name}_temperature_alerts',
    f'{my_name}_humidity_alerts',
]

# Параметри топіків
num_partitions = 2
replication_factor = 1

# Створення топіків
new_topics = []
for topic_name in topics_to_create:
    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    new_topics.append(new_topic)

# Створення всіх топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"IoN topics created successfully:")
    for topic_name in topics_to_create:
        print(f'\t{topic_name}')
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
print("\nThe list of topics created:")
existing_topics = admin_client.list_topics()
for topic in existing_topics:
    if my_name in topic:
        print(f'\t{topic}')

# Закриття зв'язку з клієнтом
admin_client.close()
