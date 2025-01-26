from confluent_kafka import Producer, KafkaError, KafkaException
import json
import time
import random
import uuid
import sys
from datetime import datetime

# Kafka configuration
config = {
    'bootstrap.servers': 'pkc-lgwgm.eastus2.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'WG66WSMCJQXGFOZ5',
    'sasl.password': 'jTiNvHIX6U1NVHZQ0QcWgOpNHOGb8potEaFQw9v8WMPelil+wxhQmaTxUpv21/Ie',
    # Add more detailed configuration
    'client.id': 'test-producer-1',
    'acks': 'all',
    'retries': 5,
    'retry.backoff.ms': 500,
    'enable.idempotence': True,
    'compression.type': 'snappy',
    'batch.size': 16384,
    'linger.ms': 0,  # Send immediately
    'request.timeout.ms': 30000,
    'delivery.timeout.ms': 120000
}

# Topic name
topic = 'sqlserver.TestDB.dbo.Customers'

# Message counter for tracking
message_counter = {'sent': 0, 'delivered': 0, 'failed': 0}

def verify_kafka_connection(producer):
    """Verify Kafka connection by attempting to get cluster metadata"""
    try:
        print("Verifying Kafka connection...")
        metadata = producer.list_topics(timeout=10)
        print(f"Successfully connected to Kafka cluster: {metadata.cluster_id}")
        print(f"Available topics: {metadata.topics}")
        if topic not in metadata.topics:
            print(f"WARNING: Topic '{topic}' not found in the cluster!")
            return False
        return True
    except KafkaException as e:
        print(f"Failed to connect to Kafka cluster: {e}")
        return False

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    global message_counter
    if err is not None:
        print(f'Message delivery failed: {err}')
        message_counter['failed'] += 1
        if isinstance(err, KafkaError):
            print(f'Kafka Error Code: {err.code()}')
            print(f'Kafka Error Name: {err.name()}')
            print(f'Kafka Error String: {err.str()}')
    else:
        message_counter['delivered'] += 1
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        print(f'Current stats - Sent: {message_counter["sent"]}, '
              f'Delivered: {message_counter["delivered"]}, '
              f'Failed: {message_counter["failed"]}')

def generate_customer_data(customer_id):
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia']
    return {
        'customer_id': str(customer_id),
        'first_name': 'Geeta',
        'last_name': random.choice(last_names),
        'email': f'geeta.{customer_id}@example.com'
    }

def produce_single_message(producer, message, operation_type, cycle, total_cycles):
    """Produce a single message with retries"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"\n[{cycle + 1}/{total_cycles}] Producing {operation_type} message: {json.dumps(message, indent=2)}")
            producer.produce(
                topic,
                key=str(uuid.uuid4()),
                value=json.dumps(message),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger delivery reports
            message_counter['sent'] += 1
            return True
        except BufferError as e:
            print(f"Buffer full. Waiting... ({e})")
            producer.poll(1)  # Flush some messages
            retry_count += 1
        except Exception as e:
            print(f"Error producing {operation_type} message: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying... (attempt {retry_count + 1}/{max_retries})")
                time.sleep(1)
            else:
                print(f"Failed to produce message after {max_retries} attempts")
                return False
    return False

def produce_messages():
    # Create Producer instance
    producer = Producer(config)
    
    # Verify connection before starting
    if not verify_kafka_connection(producer):
        print("Failed to verify Kafka connection. Exiting...")
        return
    
    total_cycles = 1000
    current_cycle = 0
    
    print(f"Starting to produce {total_cycles} cycles of INSERT-UPDATE-DELETE messages...")
    
    while current_cycle < total_cycles:
        try:
            customer_id = 1001 + current_cycle
            
            # 1. INSERT
            customer_data = generate_customer_data(customer_id)
            insert_message = {
                'operation': 'INSERT',
                'before': None,
                'after': customer_data
            }
            if not produce_single_message(producer, insert_message, 'INSERT', current_cycle, total_cycles):
                continue
            producer.flush()
            time.sleep(1)
            
            # 2. UPDATE
            updated_data = generate_customer_data(customer_id)
            updated_data['email'] = f'geeta.{customer_id}.updated@example.com'
            update_message = {
                'operation': 'UPDATE',
                'before': customer_data,
                'after': updated_data
            }
            if not produce_single_message(producer, update_message, 'UPDATE', current_cycle, total_cycles):
                continue
            producer.flush()
            time.sleep(1)
            
            # 3. DELETE
            delete_message = {
                'operation': 'DELETE',
                'before': updated_data,
                'after': None
            }
            if not produce_single_message(producer, delete_message, 'DELETE', current_cycle, total_cycles):
                continue
            producer.flush()
            time.sleep(1)
            
            current_cycle += 1
            print(f"\nCompleted cycle {current_cycle}/{total_cycles}")
            print(f"Message stats - Sent: {message_counter['sent']}, "
                  f"Delivered: {message_counter['delivered']}, "
                  f"Failed: {message_counter['failed']}")
            
        except KeyboardInterrupt:
            print("\nStopping message production...")
            break
        except Exception as e:
            print(f"Error in cycle {current_cycle}: {e}")
            print("Waiting 5 seconds before continuing...")
            time.sleep(5)
    
    # Final flush and stats
    print("\nFlushing final messages...")
    producer.flush(timeout=30)
    print(f"\nFinal stats - Sent: {message_counter['sent']}, "
          f"Delivered: {message_counter['delivered']}, "
          f"Failed: {message_counter['failed']}")

if __name__ == "__main__":
    print("Starting message production...")
    print(f"Producing messages to topic: {topic}")
    print("Will produce 1000 cycles of INSERT-UPDATE-DELETE messages")
    print("Each cycle will use 'Geeta' as the first name")
    print("Press Ctrl+C to stop")
    produce_messages()
    print("\nFinished producing all messages")
