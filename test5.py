from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from kafka import KafkaProducer, KafkaConsumer
import json
from threading import Thread

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost:5433/myfl'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Define the Task2 model
class Test(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(200), nullable=False)

# Kafka producer configuration
kafka_bootstrap_servers = 'localhost:9092'

# Kafka consumer configuration
kafka_topic = 'user-logins'
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=[kafka_bootstrap_servers],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Background task to consume Kafka messages and save to the database
def consume_and_save():
    for message in consumer:
        data = message.value

        # Save data to the database
        record = Test(title=data['title'])
        db.session.add(record)
        db.session.commit()