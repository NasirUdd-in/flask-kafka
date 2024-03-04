from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

@app.route('/send_message', methods=['POST'])
def send_message():
    message = request.form.get('message', '')  # Use get method with a default value
    if message:
        producer.send('user-logins', message.encode('utf-8'))
        return 'Message sent successfully'
    else:
        return 'No message provided', 400  # Return a 400 Bad Request if no message is provided

if __name__ == '__main__':
    app.run(debug=True)
