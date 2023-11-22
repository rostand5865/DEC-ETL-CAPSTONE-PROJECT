from kafka import KafkaProducer
import json
import time
import datetime
from faker import Faker
from collections import Counter  
import pandas as pd
import json
import sys
from confluent_kafka import Producer
from numpyencoder import NumpyEncoder



#df = pd.read_csv ('Motor_Vehicle_Collisions_-_Crashes_20231117.csv', sep=',')
df = pd.read_csv ('/sampleData/new.csv', sep=',')
collison_id = int(df['COLLISION_ID'].max())
KAFKA_TOPIC = "collisions_crashes"
BOOTSTRAP_SERVER = 'localhost:9092'
fake = Faker()

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def get_random_crash_record(collisionId):
    typeCode1 = list(Counter(df['VEHICLE TYPE CODE 1'].astype(str)).keys())
    typeCode2 = list(Counter(df['VEHICLE TYPE CODE 2'].astype(str)).keys())
    typeCode3 = list(Counter(df['VEHICLE TYPE CODE 3'].astype(str)).keys())
    typeCode4 = list(Counter(df['VEHICLE TYPE CODE 4'].astype(str)).keys())
    typeCode5 = list(Counter(df['VEHICLE TYPE CODE 5'].astype(str)).keys())

    contributing_factor_vehicle_1 = list(Counter(df['CONTRIBUTING FACTOR VEHICLE 1'].astype(str)).keys())
    contributing_factor_vehicle_2 = list(Counter(df['CONTRIBUTING FACTOR VEHICLE 2'].astype(str)).keys())
    contributing_factor_vehicle_3 = list(Counter(df['CONTRIBUTING FACTOR VEHICLE 3'].astype(str)).keys())
    contributing_factor_vehicle_4 = list(Counter(df['CONTRIBUTING FACTOR VEHICLE 4'].astype(str)).keys())
    contributing_factor_vehicle_5 = list(Counter(df['CONTRIBUTING FACTOR VEHICLE 5'].astype(str)).keys())
    borough = ['BROOKLYN', 'BRONX', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
    number_of_persons_injured = fake.random_int(min=6, max=20)
    number_of_pedestrians_injured = fake.random_int(min=7, max=15)
    number_of_cyclist_injured = fake.random_int(min=3, max=5)
    number_of_motorist_injured = fake.random_int(min=2, max=3)
    time = "".join([str(fake.random_int(min=0,max=24)), ":",str(fake.random_int(min=0,max=60))])
    streetNames = [line.strip() for line in open("StreetNames.txt", 'r')]
    latitude = str(fake.latitude())
    longitude = str(fake.longitude())
    return {
        "crash_date":"".join([fake.date_time_between(start_date='-9y', end_date='now', tzinfo=None).strftime('%Y-%m-%d')," 00:00:00.000"]),
        "crash_time": time,
        "borough":borough[fake.random_int(min=0, max=len(borough)-1)],
        "zip code":fake.random_int(min=11111, max=99999),
        "latitude":latitude,
        "longitude":longitude,
        "location":{
            "latitude":latitude,
            "longitude":longitude
        },
        "on_street_name":streetNames[fake.random_int(min=0, max=len(streetNames)-1)],
        "off_street_name":streetNames[fake.random_int(min=0, max=len(streetNames)-1)],
        "number_of_persons_injured": number_of_persons_injured,
        "number_of_persons_killed": number_of_persons_injured - fake.random_int(min=0, max=6),
        "number_of_pedestrians_injured": number_of_pedestrians_injured,
        "number_of_pedestrians_killed": number_of_pedestrians_injured - fake.random_int(min=0, max=7),
        "number_of_cyclist_injured": number_of_cyclist_injured,
        "number_of_cyclist_killed": number_of_cyclist_injured - fake.random_int(min=0, max=3),
        "number_of_motorist_injured": number_of_motorist_injured,
        "number_of_motorist_killed":number_of_motorist_injured - fake.random_int(min=0, max=2),
        "contributing_factor_vehicle_1": contributing_factor_vehicle_1[fake.random_int(min=0, max=len(contributing_factor_vehicle_1)-1)],
        "contributing_factor_vehicle_2": contributing_factor_vehicle_2[fake.random_int(min=0, max=len(contributing_factor_vehicle_2)-1)],
        "contributing_factor_vehicle_3": contributing_factor_vehicle_3[fake.random_int(min=0, max=len(contributing_factor_vehicle_3)-1)],
        "contributing_factor_vehicle_4": contributing_factor_vehicle_4[fake.random_int(min=0, max=len(contributing_factor_vehicle_4)-1)],
        "contributing_factor_vehicle_5": contributing_factor_vehicle_5[fake.random_int(min=0, max=len(contributing_factor_vehicle_5)-1)],
        "collision_id": collisionId,
        "vehicle_type_code1": typeCode1[fake.random_int(min=0, max=len(typeCode1)-1)],
        "vehicle_type_code2": typeCode2[fake.random_int(min=0, max=len(typeCode2)-1)],
        "vehicle_type_code3": typeCode3[fake.random_int(min=0, max=len(typeCode3)-1)],
        "vehicle_type_code4": typeCode4[fake.random_int(min=0, max=len(typeCode4)-1)],
        "vehicle_type_code5": typeCode5[fake.random_int(min=0, max=len(typeCode5)-1)]
    }

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

producer = Producer(read_ccloud_config("client.properties"))

if __name__ == "__main__":
    while 1:
        collison_id = collison_id +1
        random_crashes = get_random_crash_record(collison_id)
        print("{}: {}".format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'), random_crashes))
        producer.produce(
            topic="collisions_crashes", 
            key=json.dumps(collison_id, default=NumpyEncoder), 
            value=json.dumps(random_crashes).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)
        time.sleep(fake.random_int(0, 3))