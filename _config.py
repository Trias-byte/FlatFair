import os 
webhook_url = 'http://localhost:8000/webhook'
backend_url = 'http://localhost:8000/'

amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

# Для тестов без Docker Compose можно использовать:
# amqp_url = "amqp://guest:guest@localhost:5672/"
