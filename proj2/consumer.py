"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

from datetime import datetime
import time
import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name

dlq_producer = Producer({'bootstrap.servers': 'localhost:29092'})

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            print(f"Subscribed to {topics}")
            while self.keep_runnning:
                #implement your logic here
                msg = self.poll(timeout=1.0)
                if msg is None: 
                    print("No msg to consume... ")
                    continue
                if msg.error(): 
                    print(f"Kafka error: {msg.error()}")
                    continue
                print(f"Processing msg {msg.value()}")
                processing_func(msg)
        finally:
            self.close()

def is_valid_employee(e: Employee) -> bool:
    try:
        if not e.emp_FN or not e.emp_LN or not e.emp_city:
            return False
        if e.emp_id <= 0:
            return False
        if e.action not in {"INSERT", "UPDATE", "DELETE"}:
            return False
        datetime.strptime(e.emp_dob, "%Y-%m-%d")
        return True
    except Exception:
        return False

def update_dst(msg):
    try:
        e = Employee(**(json.loads(msg.value())))

        if not is_valid_employee(e): 
            raise ValueError("Validation failed for employee data")
        
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', # change this port number to align with the docker compose file
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        #your logic goes here
        if e.action == 'DELETE': 
            cur.execute(
                f"DELETE FROM employees WHERE emp_id = {e.emp_id};"
            )
        elif e.action == 'UPDATE': 
            cur.execute(
                "UPDATE employees SET first_name = %s, last_name = %s, dob = %s, city = %s WHERE emp_id = %s;", 
                (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_id)
            )
        elif e.action == 'INSERT': 
            cur.execute(
                "INSERT INTO employees (emp_id, first_name, last_name, dob, city) VALUES (%s, %s, %s, %s, %s);", 
                (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city)
            )
        else: 
            raise ValueError(f"Unsupported action type: {e.action}")
        cur.close()
    except Exception as err:
        print(f"DLQ error: {err}")
        dlq_payload = json.dumps({
            "original value": msg.value().decode('utf-8') if isinstance(msg.value(), bytes) else msg.value(), 
            "error": str(err)
        })
        dlq_producer.produce("bf_employee_cdc_dlq", value=dlq_payload)
        dlq_producer.flush()

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='employees_test') 
    print(f"Connected to {consumer.conf}")
    consumer.consume([employee_topic_name], update_dst)