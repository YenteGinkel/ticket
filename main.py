import base64, json, uuid
from google.cloud import firestore, bigquery

# STEP 1: Helper function to query BigQuery
def get_ticket_limits(event_id):
    client = bigquery.Client()
    query = """
        SELECT capacity, issued
        FROM `de2024-collin.festivaldb.sessions`
        WHERE event_id = @event_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    for row in result:
        return row.capacity, row.issued
    return None, None

# STEP 2: Helper to update issued count
def increment_issued(event_id):
    client = bigquery.Client()
    query = """
        UPDATE `your-project-id.your_dataset.sessions`
        SET issued = issued + 1
        WHERE event_id = @event_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
        ]
    )
    client.query(query, job_config=job_config).result()

# STEP 3: Cloud Function handler
def issue_ticket(event, context):
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    event_id = data['event_id']
    user_id = data['user_id']
    ticket_type = data['ticket_type']

    # Step 1: Query BigQuery
    capacity, issued = get_ticket_limits(event_id)
    if capacity is None or issued is None:
        print("Event not found in BigQuery")
        return
    if issued >= capacity:
        print("Capacity reached")
        return

    # Step 2: Create ticket in Firestore
    ticket_id = str(uuid.uuid4())
    qr_code = f"QR-{ticket_id}"
    db = firestore.Client()
    db.collection('tickets').document(ticket_id).set({
        'ticket_id': ticket_id,
        'user_id': user_id,
        'event_id': event_id,
        'ticket_type': ticket_type,
        'qr_code': qr_code,
        'status': 'valid'
    })

    # Step 3: Update BigQuery
    increment_issued(event_id)
    print(f"Ticket {ticket_id} issued to {user_id}")
