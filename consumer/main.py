from quixstreams import Application
import os
import boto3
import pandas as pd
import io
from datetime import datetime
from dotenv import load_dotenv
import json
import time

load_dotenv()

# --- AWS S3 Setup ---
s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get('AWS_SECRET_KEY'),
    region_name=os.environ.get('AWS_REGION')
)
bucket_name = os.environ.get('BUCKET_NAME')

# --- Kafka Setup ---
app = Application(
    broker_address=os.environ.get("Quix__Broker__Address"),
    consumer_group="stock-group",
    auto_offset_reset="earliest"  
)

# Create a consumer for the "stocks" topic
topic = app.topic("stocks")
consumer = app.get_consumer()

# --- Function to Save to S3 ---
def save_to_s3(data):
    df = pd.DataFrame(data)
    now = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
    file_key = f"stock-data/stock_{now}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket_name, file_key)
    print(f"✅ Uploaded {file_key} to S3")



# At the top
message_buffer = []
BATCH_SIZE = 100
SAVE_INTERVAL = 30  # seconds
last_save_time = time.time()

# In process_message
def process_message(message):
    global last_save_time
    try:
        data = json.loads(message.value())

        record = {
            "symbol": data.get("symbol"),
            "price": data.get("price"),
            "size": data.get("size"),
            "side": data.get("side"),
            "action": data.get("action"),
            "ts_event": data.get("ts_event"),
            "ts_recv": data.get("ts_recv"),
            "ts_ingest": datetime.utcnow().isoformat()
        }

        message_buffer.append(record)

        # Save if buffer is large or time passed
        if len(message_buffer) >= BATCH_SIZE or (time.time() - last_save_time) >= SAVE_INTERVAL:
            save_to_s3(message_buffer.copy())
            message_buffer.clear()
            last_save_time = time.time()

        return True

    except Exception as e:
        print(f"⚠️ Error: {e}")
        return False

# --- Consume and Process Messages ---
def main():
    print("📥 Starting consumer for topic 'stocks'...")
    consumer.subscribe([topic.name])
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                time.sleep(0.1)
                continue
                
            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue
                
            # Process the message
            success = process_message(msg)
            
            # Only commit if processing was successful
            if success:
                try:
                    # Store the offset to commit later
                    consumer.store_offsets(message=msg)
                except Exception as e:
                    print(f"⚠️ Failed to store offset: {e}")
                    
    except KeyboardInterrupt:
        print("\n👋 Shutting down consumer gracefully...")
    finally:
        try:
            # Commit any stored offsets before closing
            consumer.commit()
        except Exception as e:
            print(f"⚠️ Final commit failed: {e}")
        finally:
            consumer.close()
            print("✅ Consumer closed")

if __name__ == "__main__":
    main()