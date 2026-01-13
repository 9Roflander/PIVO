"""
Kafka Service - Handles Event Streaming for PIVO Ingestion
"""
import json
import time
from typing import Optional, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
from ..config import Config

TOPIC_NAME = "pivo-commit-events"

class PivoKafkaProducer:
    def __init__(self, config: Config):
        self.bootstrap_servers = config.kafka_bootstrap_servers
        self.producer = None
        
    def connect(self):
        """Connect to Kafka with retries."""
        for i in range(5):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print(f"[KAFKA] Connected to producer at {self.bootstrap_servers}")
                return
            except Exception as e:
                print(f"[KAFKA] Connection failed (attempt {i+1}/5): {e}")
                time.sleep(2)
        print("[KAFKA] Could not connect to Kafka. Running safely without it (events won't be sent).")

    def produce_commit_event(self, metadata: dict, hdfs_path: str):
        """Send commit metadata event."""
        if not self.producer:
            return

        payload = {
            "event_type": "COMMIT_INGESTED",
            "commit_hash": metadata.commit_hash,
            "repo_name": metadata.repo_name,
            "metadata": {
                "author": metadata.author,
                "message": metadata.commit_message,
                "timestamp": metadata.commit_timestamp,
                "files_changed": metadata.files_changed,
                "additions": metadata.additions,
                "deletions": metadata.deletions,
                "branch": metadata.branch
            },
            "hdfs_path": hdfs_path
        }
        
        future = self.producer.send(TOPIC_NAME, payload)
        try:
            record_metadata = future.get(timeout=10)
            print(f"[KAFKA] Event sent to {record_metadata.topic} partition {record_metadata.partition}")
        except Exception as e:
            print(f"[KAFKA] Failed to send event: {e}")


class PivoKafkaConsumer:
    def __init__(self, config: Config):
        self.bootstrap_servers = config.kafka_bootstrap_servers
        self.consumer = None
        self.running = False
        
    def start(self, callback: Callable[[dict], None]):
        """Start listening loop. Callback processes the message."""
        print(f"[KAFKA] Connecting Consumer to {self.bootstrap_servers}...")
        try:
            self.consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='pivo-metadata-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except Exception as e:
            print(f"[KAFKA] Consumer connection failed: {e}")
            return

        print(f"[KAFKA] Listening on topic '{TOPIC_NAME}'...")
        self.running = True
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                event = message.value
                print(f"[KAFKA] Received event: {event.get('event_type')} for {event.get('repo_name')}")
                
                try:
                    callback(event)
                except Exception as e:
                    print(f"[ERROR] Error processing event: {e}")
                    
        except KeyboardInterrupt:
            print("[KAFKA] Stopping consumer...")
        finally:
            self.consumer.close()
