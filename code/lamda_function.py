 import boto3
 import json
 import urllib3
 import pandas as pd

 # S3 bucket names
 raw_data_bucket_name = "apprentice-training-rabin-ml-raw-dev"
 cleaned_data_bucket_name = "apprentice-training-rabin-ml-cleaned-dev"

 s3 = boto3.client('s3')

 def lambda_handler(event, context):
     http = urllib3.PoolManager()
    
     url = "https://weatherapi-com.p.rapidapi.com/current.json"
     querystring = {"q": "53.1,-0.13"}
     headers = {
         "X-RapidAPI-Key": "338597015emsh089306e2a946537p181fbajsnf4c60ed43bae",
         "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
     }
   
     # Make API call
     response = http.request('GET', url, headers=headers, fields=querystring)
     data = json.loads(response.data.decode('utf-8'))
     # Save initial raw data to S3
     s3.put_object(Bucket=raw_data_bucket_name, Key='raw_data.json', Body=json.dumps(data))
    
    # Perform data transformation using pandas
     transformed_data = {
         "Location": data["location"]["name"],
         "Temperature_C": data["current"]["temp_c"],
         "Temperature_F": data["current"]["temp_f"],
         "Condition": data["current"]["condition"]["text"],
         "Wind_mph": data["current"]["wind_mph"],
         "Pressure_mb": data["current"]["pressure_mb"],
         "Humidity": data["current"]["humidity"]
     }
    
     cleaned_data = pd.DataFrame([transformed_data])
    
     # Check for null values and remove duplicates
     cleaned_data.dropna(inplace=True)
     cleaned_data.drop_duplicates(inplace=True)
   
     # Convert cleaned data to JSON format
     cleaned_data_json = cleaned_data.to_json(orient="records")
   
     # Save cleaned data to another S3 bucket
     s3.put_object(Bucket=cleaned_data_bucket_name, Key='cleaned_data.json', Body=cleaned_data_json)
    
     return {
         'statusCode': 200,
         'body': json.dumps('Hello from Lambda!')
     }