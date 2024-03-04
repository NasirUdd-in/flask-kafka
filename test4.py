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
producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_servers],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

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

# Flask route to handle POST requests and send data to Kafka
@app.route('/send_data', methods=['POST'])
def send_data():
    try:
        data = request.get_json()
        title = data.get('title')

        if title:
            # Produce Kafka message
            kafka_data = {'title': title}
            producer.send(kafka_topic, value=kafka_data)

            return jsonify({'message': 'Data sent successfully!'})
        else:
            return jsonify({'error': 'Title is required'}), 400

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Flask route to handle GET requests and retrieve data from the database
@app.route('/get_data', methods=['GET'])
def get_data():
    try:
        # Retrieve data from the database
        data = Test.query.all()
        result = [{'id': record.id, 'title': record.title} for record in data]

        return jsonify({'data': result})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Start the Kafka consumer in a separate thread
    consumer_thread = Thread(target=consume_and_save)
    consumer_thread.start()

    # Run the Flask application
    app.run(debug=True, port=5006)
