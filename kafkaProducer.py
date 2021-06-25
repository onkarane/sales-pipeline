from kafka import KafkaProducer
import time
import json
import random

#set the producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

#set the states
states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ]
#set the item category
category = ['Fashion', 'Furniture', 'Toys', 'Electronics', 'Food Items']

#set platform category
platform = ['Online', 'Offline']

#function to generate json data
def generate_data():
    """
    Function to randomly choose data from the lists
    and return randomly generated json.
    """

    #randomly select values
    s = random.choice(states)
    c = random.choice(category)
    p = random.choice(platform)

    #create json
    data = {'state' : s, 'category': c, 'platform': p}
    json_data = json.dumps(data).encode('utf-8')

    return json_data

counter = 1

#loop and call the function
while True:

    data = generate_data()
    #producer.send('sales', data)
    producer.send('sales', data)
    time.sleep(3)
    counter += 1
    print(str(counter) + ' messages sent!')
    if counter == 100:
        break

