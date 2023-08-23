import json
import urllib3
import boto3
import psycopg2
import os

def lambda_handler(event, context):
    # API Configuration
    url = "https://wft-geo-db.p.rapidapi.com/v1/geo/adminDivisions?limit=10"
    headers = {
        "X-RapidAPI-Key": "338597015emsh089306e2a946537p181fbajsnf4c60ed43bae",
        "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com"
    }
    
    # Initialize urllib3 and make API request
    http = urllib3.PoolManager()
    response = http.request('GET', url, headers=headers)
    data = json.loads(response.data)
    
    # S3 Configuration
    s3_client = boto3.client('s3')
    raw_bucket_name = 'apprentice-training-rabin-ml-raw-dev'
    cleaned_bucket_name = 'apprentice-training-rabin-ml-cleaned-dev'
    
    # Store raw data in S3
    raw_data_key = 'raw_data1.json'
    s3_client.put_object(Bucket=raw_bucket_name, Key=raw_data_key, Body=json.dumps(data))
    
    # Data Transformation
    transformed_data = []
    for entry in data['data']:
        transformed_entry = {
            'Country': entry.get('country', None),
            'CountryCode': entry.get('countryCode', None),
            'Latitude': entry.get('latitude', None),
            'Longitude': entry.get('longitude', None),
            'RegionName': entry.get('name', None),
            'Population': entry.get('population', None),
            'Region': entry.get('region', None)
        }
        transformed_data.append(transformed_entry)
    
    # Remove entries with null values
    cleaned_data = [entry for entry in transformed_data if all(value is not None for value in entry.values())]
    
    # Remove duplicate entries
    cleaned_data = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in cleaned_data)]
    
    # Store cleaned data in S3
    cleaned_data_key = 'cleaned_data1.json'
    s3_client.put_object(Bucket=cleaned_bucket_name, Key=cleaned_data_key, Body=json.dumps(cleaned_data))
    
    
    # Connect to RDS
    conn = psycopg2.connect(
        host=os.environ['host'],
        database=os.environ['database'],
        user=os.environ['user'],
        password=os.environ['password']
    )
    
    cursor = conn.cursor()
    
    # Create table if not exists
    create_table_query = """
    CREATE TABLE apprentice_training_rabin_table (
        id SERIAL PRIMARY KEY,
        country VARCHAR,
        country_code VARCHAR,
        latitude FLOAT,
        longitude FLOAT,
        region_name VARCHAR,
        population INTEGER,
        region VARCHAR
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Insert values into the table
    insert_query = """
    INSERT INTO apprentice_training_rabin_table (country, country_code, latitude, longitude, region_name, population, region)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    for entry in cleaned_data:
        cursor.execute(insert_query, (
            entry['Country'],
            entry['CountryCode'],
            entry['Latitude'],
            entry['Longitude'],
            entry['RegionName'],
            entry['Population'],
            entry['Region']
        ))
    conn.commit()
    
    cursor.close()
    conn.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data fetching, transformation, and storage complete.')
    }
