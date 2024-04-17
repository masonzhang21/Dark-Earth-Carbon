import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os 
from datetime import datetime
import math
def init_firebase():
    cred = credentials.Certificate("serviceAccountKey.json") # Update the path
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db
def get_time(file_path): 
    #last_write_time = os.path.getmtime(csv_file_path)
    last_write_time = os.stat(file_path).st_birthtime
    timestamp = datetime.fromtimestamp(last_write_time)
    # Format the datetime object to a string in your preferred format
    formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


def do_stuff(): 
    db = init_firebase()
    collection_name = 'plc-data' 
    csv_file_path = 'plc_data.csv' 
    # Create, filter, and rename pandas df 
    df = pd.read_csv(csv_file_path, encoding="ISO-8859-1")
    df = df.drop(df.columns[0], axis=1)
    new_cols = ["timestamp", "temp1", "temp2", "temp3", "temp4", "pressure1", "pressure2", "pressure3"]
    df.columns = new_cols
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.tz_localize('Etc/GMT-3')
    most_recent_doc = db.collection(collection_name).order_by("timestamp", direction=firestore.Query.DESCENDING).limit(1).get()
    if len(most_recent_doc) > 0:
        cutoff_timestamp = most_recent_doc[0].to_dict()["timestamp"]
        df = df[df["timestamp"] > cutoff_timestamp]
    # Upload all valid data rows to Firestore in batches
    BATCH_SIZE = 500
    # Split DataFrame into chunks
    num_chunks = math.ceil(len(df) / BATCH_SIZE)
    #num_chunks = 1
    for i in range(int(num_chunks)):
        start = i * BATCH_SIZE
        end = (i + 1) * BATCH_SIZE
        batch = db.batch()
        # Process each chunk
        for _, row in df.iloc[start:end].iterrows():
            timestamp_str = row["timestamp"].strftime('%Y-%m-%d %H:%M:%S')
            doc_ref = db.collection(collection_name).document(timestamp_str)  # Assuming 'id' as document ID
            batch.set(doc_ref, row.to_dict())
        # Commit the batch
        batch.commit()
        print(f'Batch {i+1}/{int(num_chunks)} committed.')

do_stuff()