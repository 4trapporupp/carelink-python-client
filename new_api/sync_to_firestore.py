
from datetime import datetime as dt
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pytz

def to_firestore_timestamp(date_str):
  date_format = "%Y-%m-%dT%H:%M:%S.%f%z"  # Adjust format to match your date strings
  d = dt.strptime(date_str, date_format)
  return d

def init_firestore():
  cred = credentials.Certificate('./serviceaccount.json')
  firebase_admin.initialize_app(cred)

latest_upload_time = dt.min.replace(tzinfo=pytz.UTC)

def uploadJsonToFirestore(json_data):
    global latest_upload_time

    db = firestore.client()
    batch = db.batch()
    number_of_items = 0

    new_latest_upload_time = latest_upload_time

    # Process sgs array
    for item in json_data['sgs']:
        item_time = to_firestore_timestamp(item['datetime'])

        if item_time <= latest_upload_time:
            continue  # Skip items older than the latest upload time

        # Update new latest upload time
        if item_time > new_latest_upload_time:
            new_latest_upload_time = item_time

        if item['sensorState'] == 'NO_ERROR_MESSAGE':
            doc_ref = db.collection('sugarglucose').document(item['datetime'])
            doc = {'sg': item['sg'], 'kind': item['kind']}
            firestore_timestamp = to_firestore_timestamp(item['datetime'])
            doc["dateTime"] = firestore_timestamp
            batch.set(doc_ref, doc)
            number_of_items += 1
        else:
            print("Skipped: " + item['datetime'] + " due to: " + item['sensorState'])

    # Process markers array
    for marker in json_data['markers']:
        marker_time = to_firestore_timestamp(marker['dateTime'])

        if marker_time <= latest_upload_time:
            continue  # Skip items older than the latest upload time

        # Update new latest upload time
        if marker_time > new_latest_upload_time:
            new_latest_upload_time = marker_time

        doc_id = marker['dateTime']
        doc_ref = None
        firestore_timestamp = to_firestore_timestamp(marker['dateTime'])

        if marker['type'] == 'MEAL':
            doc_ref = db.collection('carbohydrates').document(doc_id)
            doc = {'amount': marker['amount']}

        elif marker['type'] == 'INSULIN':
            total_amount = marker.get('deliveredFastAmount', 0) + marker.get('deliveredExtendedAmount', 0)
            doc_ref = db.collection('insulin').document(doc_id)
            doc = {
                'deliveredFastAmount': marker.get('deliveredFastAmount', 0),
                'deliveredExtendedAmount': marker.get('deliveredExtendedAmount', 0),
                'amount': total_amount,
                'activationType': marker.get('activationType', '')
            }

        elif marker['type'] == 'AUTO_BASAL_DELIVERY':
            doc_ref = db.collection('insulin').document(doc_id)
            doc = {
                'amount': marker.get('bolusAmount', 0),
                'activationType': 'AUTO_BASAL'
            }

        else:
            print("New type", marker['type'])

        if doc_ref:
            doc["dateTime"] = firestore_timestamp
            batch.set(doc_ref, doc)
            number_of_items += 1

    # Number of items in the batch
    print("Number of items in the batch: ", number_of_items)
    # Commit the batch
    batch.commit()

    # Update the latest upload timestamp in memory
    latest_upload_time = new_latest_upload_time

    print(dt.now())
    print("Batch data upload to Firebase Firestore successful.")