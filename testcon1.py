from kafka import KafkaConsumer
from flask import Flask

app = Flask(__name__)
consumer = KafkaConsumer('user-logins', bootstrap_servers=['localhost:9092'])

@app.route('/get_message')
def get_message():
    message = next(consumer)
    return message.value.decode('utf-8')

if __name__ == '__main__':
    app.run(debug=True, port=5001)  # Specify the desired port, e.g., 5001
