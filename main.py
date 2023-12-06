import simplejson as json

import requests
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'


def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"


def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&seed=voting_app')
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        party_affiliation = f"Party {chr(65 + candidate_number % total_parties)}"  # Party A, B, C, etc.
        return {
            "candidate_id": user_data['login']['uuid'],
            "name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": party_affiliation,
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"


# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

if __name__ == "__main__":
    producer = SerializingProducer(
        {
            'bootstrap.servers': 'localhost:9092',
        }
    )

    # Simulate Candidate Data Generation and Sending to Kafka
    candidates = []
    for i in range(2):
        candidate_data = generate_candidate_data(i, 3)
        candidates.append(candidate_data)
        producer.produce(
            candidates_topic,
            key=candidate_data["candidate_id"],
            value=json.dumps(candidate_data)
        )

    # Simulate Voter Data Generation and Sending to Kafka
    # This can be a loop or triggered based on certain conditions
    for i in range(10000):
        voter_data = generate_voter_data()
        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data)
        )

        producer.flush()

    # print(json.dumps(candidates, indent=4))

    # while True:
    # voter_data = generate_voter_data()
    # print(json.dumps(voter_data, indent=4))
    # time.sleep(1)  # Simulate real-time data generation
