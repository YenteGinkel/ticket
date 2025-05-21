import base64, json, uuid
from google.cloud import firestore, bigquery

# STEP 1: Get capacity and issued from BigQuery
def get_ticket_limits(session_id):
    client = bigquery.Client()
    query = """
        SELECT capacity, issued
        FROM `de2024-collin.festivaldb.sessions`
        WHERE id = @session_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    for row in result:
        return row.capacity, row.issued
    return None, None

# STEP 2: Increment issued in BigQuery
def increment_issued(session_id):
    client = bigquery.Client()
    query = """
        UPDATE `de2024-collin.festivaldb.sessions`
        SET issued = issued + 1
        WHERE id = @session_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
        ]
    )
    client.query(query, job_config=job_config).result()

# STEP 3: Entry point for Cloud Function
def issue_ticket(event, context):
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    session_id = data['session_id']
    user_id = data['user_id']
    ticket_type = data['ticket_type']

    # Step 1: check capacity
    capacity, issued = get_ticket_limits(session_id)
    if capacity is None or issued is None:
        print("Session not found in BigQuery")
        return
    if issued >= capacity:
        print("Capacity reached")
        return

    # Step 2: issue ticket in Firestore
    ticket_id = str(uuid.uuid4())
    qr_code = f"QR-{ticket_id}"
    db = firestore.Client()
    db.collection('tickets').document(ticket_id).set({
        'ticket_id': ticket_id,
        'user_id': user_id,
        'session_id': session_id,
        'ticket_type': ticket_type,
        'qr_code': qr_code,
        'status': 'valid'
    })

    # Step 3: update BigQuery
    increment_issued(session_id)
    print(f"✅ Ticket {ticket_id} issued to {user_id} for session {session_id}")
import base64, json, uuid
from google.cloud import firestore, bigquery

# STEP 1: Get capacity and issued from BigQuery
def get_ticket_limits(session_id):
    client = bigquery.Client()
    query = """
        SELECT capacity, issued
        FROM `de2024-collin.festivaldb.sessions`
        WHERE id = @session_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    for row in result:
        return row.capacity, row.issued
    return None, None

# STEP 2: Increment issued in BigQuery
def increment_issued(session_id):
    client = bigquery.Client()
    query = """
        UPDATE `de2024-collin.festivaldb.sessions`
        SET issued = issued + 1
        WHERE id = @session_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
        ]
    )
    client.query(query, job_config=job_config).result()

# STEP 3: Entry point for Cloud Function
def issue_ticket(event, context):
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    session_id = data['session_id']
    user_id = data['user_id']
    ticket_type = data['ticket_type']

    # Step 1: check capacity
    capacity, issued = get_ticket_limits(session_id)
    if capacity is None or issued is None:
        print("Session not found in BigQuery")
        return
    if issued >= capacity:
        print("Capacity reached")
        return

    # Step 2: issue ticket in Firestore
    ticket_id = str(uuid.uuid4())
    qr_code = f"QR-{ticket_id}"
    db = firestore.Client()
    db.collection('tickets').document(ticket_id).set({
        'ticket_id': ticket_id,
        'user_id': user_id,
        'session_id': session_id,
        'ticket_type': ticket_type,
        'qr_code': qr_code,
        'status': 'valid'
    })

    # Step 3: update BigQuery
    increment_issued(session_id)
    print(f"Ticket {ticket_id} issued to {user_id} for session {session_id}")
