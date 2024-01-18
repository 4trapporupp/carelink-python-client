
from datetime import datetime as dt
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def to_firestore_timestamp(date_str):
  date_format = "%Y-%m-%dT%H:%M:%S.%f%z"  # Adjust format to match your date strings
  d = dt.strptime(date_str, date_format)
  return d

def init_firestore():
  cred = credentials.Certificate('./serviceaccount.json')
  firebase_admin.initialize_app(cred)

def uploadJsonToFirestore(json_data):
  db = firestore.client()
  batch = db.batch()

  # Process sgs array
  for item in json_data['sgs']:
    if item['sensorState'] == 'NO_ERROR_MESSAGE':
      doc_ref = db.collection('sugarglucose').document(item['datetime'])
      doc = {'sg': item['sg'], 'kind': item['kind']}
      firestore_timestamp = to_firestore_timestamp(item['datetime'])
      doc["dateTime"] = firestore_timestamp
      batch.set(doc_ref, doc)
    else:
      print("Skipped: " + item['datetime'] + "due to: " + item['sensorState'])

  # Process markers array
  for marker in json_data['markers']:
    doc_id = marker['dateTime']
    doc_ref = None
    firestore_timestamp = to_firestore_timestamp(marker['dateTime'])

    if marker['type'] == 'MEAL':
      doc_ref = db.collection('carbohydrates').document(doc_id)
      doc = {'amount': marker['amount']}

    elif marker['type'] == 'INSULIN':
      total_amount = marker.get('deliveredFastAmount', 0) + marker.get(
          'deliveredExtendedAmount', 0)
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
          #save datetime as date object
          'amount': marker.get('bolusAmount', 0),
          'activationType': 'AUTO_BASAL'
      }

    else:
      print("New type", marker['type'])

    doc["dateTime"] = firestore_timestamp
    #check if doc_ref is not None or doc
    if doc_ref:
      batch.set(doc_ref, doc)

  # Commit the batch
  batch.commit()
  #print date
  print(dt.now())
  print("Batch data upload to Firebase Firestore successful.")