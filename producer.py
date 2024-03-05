import json
from datetime import datetime
from flask import Flask, request, jsonify, session
from kafka import KafkaProducer


app = Flask(__name__)
app.secret_key = "your_secret_key"

producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(2, 7, 0),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# Inside your Flask routes (e.g., /update_profile, /access_page)
@app.route('/update_profile', methods=['POST'])
def update_profile():
    data = request.get_json()
    if data and 'username' in data and 'profile_data' in data:
        username = data['username']
        profile_data = data['profile_data']
        ip_address = request.remote_addr  # Retrieve IP address
       

        # Send user activity message to Kafka with IP address for update_profile event
        producer.send('user_activity', {'event_type': 'update_profile', 'username': username, 'ip_address': ip_address, 'profile_data': profile_data, 'timestamp': str(datetime.now()), 'project':  "projectone"})
        producer.flush()  # Ensure all messages are sent

        return jsonify({'message': 'Profile updated successfully'})
    else:
        return jsonify({'message': 'Username and profile_data are required'}), 400

@app.route('/access_page', methods=['GET'])
def access_page():
    if 'username' in session:
        username = session['username']
        ip_address = request.remote_addr  # Retrieve IP address

        # Send user activity message to Kafka with IP address for access_page event
        producer.send('user_activity', {'event_type': 'access_page', 'username': username, 'ip_address': ip_address, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent

        return jsonify({'message': 'Page accessed successfully'})
    else:
        return jsonify({'message': 'You are not logged in'}), 401

@app.route('/change_password', methods=['POST'])
def change_password():
    data = request.get_json()
    if data and 'username' in data and 'password' in data:
        username = data['username']
        # Here, you could add additional validation if required
        session['username'] = username
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'password changed', 'username': username,'ip_address': request.remote_addr, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Password Changed'})
    else:
        return jsonify({'message': 'Username and password are required'}), 400
# Other routes remain unchanged...

if __name__ == '__main__':
    app.run(debug=True)
    
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    if data and 'username' in data and 'password' in data:
        username = data['username']
        # Here, you could add additional validation if required
        session['username'] = username
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'register', 'username': username,'ip_address': request.remote_addr, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Congrats, Registration successful'})
    else:
        return jsonify({'message': 'Username and password are required'}), 400



@app.route('/signout', methods=['POST'])
def signout():
    if 'username' in session:
        username = session.pop('username')
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'logout', 'username': username,'ip_address': request.remote_addr, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Logout successful'})
    else:
        return jsonify({'message': 'You are not logged in'}), 401


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if data and 'username' in data and 'password' in data:
        username = data['username']
        # Here, you could add additional validation if required
        session['username'] = username
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'login', 'username': username,'ip_address': request.remote_addr, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Login successful'})
    else:
        return jsonify({'message': 'Username and password are required'}), 400
# Other routes remain unchanged...

if __name__ == '__main__':
    app.run(debug=True)