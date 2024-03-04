from kafka import KafkaConsumer
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost:5433/myflask'
db = SQLAlchemy(app)

class Task2(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(200), nullable=False)

with app.app_context():
    try:
        db.create_all()
    except Exception as e:
        print(f"Error creating tables: {str(e)}")

consumer = KafkaConsumer('user-logins', bootstrap_servers=['localhost:9092'])

@app.route('/get_message')
def get_message():
    message = next(consumer)
    test = message.value.decode('utf-8')

    # Assuming you want to save the Kafka message to the Task2 table
    new_task = Task2(title=test)
    db.session.add(new_task)
    db.session.commit()

    return jsonify({'message': 'Message saved successfully', 'content': test})

if __name__ == '__main__':
    app.run(debug=True, port=5001)


@app.route('/get_message')
def get_message():
    message = next(consumer)
    test = message.value.decode('utf-8')

    # Assuming you want to save the Kafka message to the Task2 table
    new_task = Task2(title=test)
    db.session.add(new_task)
    db.session.commit()

    return jsonify({'message': 'Message saved successfully', 'content': test})

if __name__ == '__main__':
    app.run(debug=True, port=5001)
