from kafka import KafkaConsumer
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:12345@localhost:5433/myfl'
db = SQLAlchemy(app)

class Test(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(200), nullable=False)

with app.app_context():
    try:
        db.create_all()
    except Exception as e:
        print(f"Error creating tables: {str(e)}")


if __name__ == '__main__':
    app.run(debug=True, port=5001)
