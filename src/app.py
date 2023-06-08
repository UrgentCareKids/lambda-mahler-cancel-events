import pandas as pd
import psycopg2
import json
import boto3
import os

# def handler(event, context):
#     ...

ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param = ssm.get_parameter(Name='uck-etl-db-prod-masterdata', WithDecryption=True )
db_request = json.loads(param['Parameter']['Value']) 


ssm_redshift = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param_redshift = ssm_redshift.get_parameter(Name='uck-etl-wave-hdc', WithDecryption=True )
db_request_redshift = json.loads(param_redshift['Parameter']['Value'])

def masterdata_conn():
    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn

def redshift_conn():
    hostname = db_request_redshift['host']
    portno = db_request_redshift['port']
    dbname = db_request_redshift['database']
    dbusername = db_request_redshift['user']
    dbpassword = db_request_redshift['password']
    conn_redshift = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn_redshift



def get_invalid_pond_visits():
    # Create a database connection
    _targetconnection = masterdata_conn()
    cursor = _targetconnection.cursor()
    # Execute the SQL query to fetch the required data
    query = "SELECT pond_visit_id FROM mahler_event_cx WHERE is_validated = false;"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    _targetconnection.close()
    # Create a pandas DataFrame from the retrieved data
    df = pd.DataFrame(rows, columns=['pond_visit_id'])
    check_cancelled_visits(df)


def check_cancelled_visits(df):
    _targetconnection = redshift_conn()
    cursor = _targetconnection.cursor()
    # Extract the pond_visit_id values from the DataFrame
    pond_visit_ids = df['pond_visit_id'].tolist()
    # Placeholder for cancelled visits and completed visits
    cancelled_visits = []
    completed_visits = []
    # Check if pond_visit_id exists in pond_visits and is_deleted = true
    for pond_visit_id in pond_visit_ids:
        query = f"SELECT visit_id FROM pond_visits WHERE visit_id = '{pond_visit_id}' AND is_deleted = true;"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            cancelled_visits.append(pond_visit_id)
        else:
            completed_visits.append(pond_visit_id)
    cursor.close()
    _targetconnection.close()
    # Update mahler_event_cx table with the cancelled visits
    update_cancelled_visits(cancelled_visits, completed_visits)
    print('completed: ', completed_visits)
    print('cancelled: ', cancelled_visits)



def update_cancelled_visits(cancelled_visits, completed_visits):
    # Create a database connection
    _targetconnection = masterdata_conn()
    cursor = _targetconnection.cursor()
    # Update is_cancelled and is_validated columns in mahler_event_cx table
    if cancelled_visits:
        cancelled_ids = ", ".join(str(visit) for visit in cancelled_visits)
        update_query = f"UPDATE mahler_event_cx SET is_cancelled = true, is_validated = true WHERE pond_visit_id IN ({cancelled_ids}) RETURNING mahler_event_id;"
        # cursor.execute(update_query)
        print(update_query)
        cancelled_mahler_event_ids = [row[0] for row in cursor.fetchall()]
        print(cancelled_mahler_event_ids)
    else:
        # Update is_validated column to true for non-cancelled visits
        completed_ids = ", ".join(str(visit) for visit in completed_visits)
        update_query = f"UPDATE mahler_event_cx SET is_validated = true WHERE is_validated = false and pond_visit_id IN ({completed_ids});"
        # cursor.execute

def post_cancelled_events_to_mahler(cancelled_mahler_event_ids):
    ...

get_invalid_pond_visits()