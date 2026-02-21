import json
import time
import random
from kafka import KafkaProducer

# Настройки
TOPIC = 'clicks'
PRODUCER_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

# Список товаров (id, категория)
PRODUCTS = [{'id': i, 'category': f'cat_{i%5}'} for i in range(1, 101)]

def generate_click():
    product = random.choice(PRODUCTS)
    return {
        'product_id': product['id'],
        'category': product['category'],
        'timestamp': int(time.time() * 1000),  # миллисекунды
        'user_id': random.randint(1000, 9999)
    }

producer = KafkaProducer(**PRODUCER_CONFIG)
print("Генератор кликов запущен. Нажмите Ctrl+C для остановки.")

try:
    while True:
        click = generate_click()
        producer.send(TOPIC, value=click)
        print(f"Отправлено: {click}")
        time.sleep(random.uniform(0.1, 0.5))  # от 100 до 500 мс
except KeyboardInterrupt:
    producer.close()
    print("Остановлено.")
