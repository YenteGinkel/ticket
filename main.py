import base64, json, uuid
from google.cloud import firestore

def issue_ticket(event, context):
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    db = firestore.Client()

    event_id = data['event_id']
    user_id = data['user_id']
    ticket_type = data['ticket_type']

    ticket_info_ref = db.collection('ticket_info').document(event_id)
    ticket_info = ticket_info_ref.get().to_dict()

    if ticket_info['issued'] >= ticket_info['capacity']:
        print("Capaciteit bereikt")
        return

    ticket_id = str(uuid.uuid4())
    qr_code = f"QR-{ticket_id}"

    db.collection('tickets').document(ticket_id).set({
        'ticket_id': ticket_id,
        'user_id': user_id,
        'event_id': event_id,
        'ticket_type': ticket_type,
        'qr_code': qr_code,
        'status': 'valid'
    })

    ticket_info_ref.update({'issued': firestore.Increment(1)})
    print(f"✅ Ticket {ticket_id} uitgegeven aan {user_id}")
