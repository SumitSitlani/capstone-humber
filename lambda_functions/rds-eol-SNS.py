import pandas as pd
import requests
import boto3
import io
from datetime import datetime

# Initialize S3 and SNS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Dictionary to cache API responses
api_cache = {}

# Function to fetch EOL data from the API with caching
def get_eol_date(engine, major_version):
    try:  
        # Check if the response is already cached
        if (engine, major_version) in api_cache:
            return api_cache[(engine, major_version)]

        # Construct API URL
        url = f"https://endoflife.date/api/{engine}/{major_version}.json"
        response = requests.get(url)
        
        # Handle API response
        if response.status_code == 200:
            eol_date = response.json().get("eol")
            # Cache the fetched data
            api_cache[(engine, major_version)] = eol_date
            return eol_date
        else:
            return None
    except:
        return None

# Function to send notification
def send_notification(message):
    try:
        # Publish message to SNS topic
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:210929744970:Capstone-EOL-SNS',
            Message=message,
            Subject="AWS RDS Expiration Notification"
        )
        print("Notification sent successfully.")
    except Exception as e:
        print(f"Error sending notification: {str(e)}")

# Function to calculate years and months difference
def calculate_years_months_diff(eol_date):
    if pd.isnull(eol_date):
        return "NA"
    else:
        diff = eol_date - datetime.now()
        if diff.days < 0:
            return "Expired"
        else:
            years = diff.days // 365
            months = (diff.days % 365) // 30
            return f"{years} years {months} months" if years > 0 else f"{months} months"


def lambda_handler(event, context):
    try:
        # Get the bucket and key where the CSV file is located
        bucket_name = 'test-bucket-capstone'
        file_key = 'rds.csv'
        
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        rds_data = pd.read_csv(io.BytesIO(response['Body'].read()))

        # Split 'engine_version' column into 'major_version' and 'minor_version' for RDS instances
        split_version_rds = rds_data['engine_version'].str.split('.', expand=True)
        rds_data['major_version'] = split_version_rds[0]
        rds_data['minor_version'] = split_version_rds[1].str[:1]  # Keep only the first character after the decimal point
        rds_data['maintenance_version'] = split_version_rds[2].str[:1]  # Keep only the first character after the decimal point

        # Initialize 'eol_date' column with None for RDS instances
        rds_data['eol_date'] = None

        # Iterate over each row in the DataFrame for RDS instances
        for index, row in rds_data.iterrows():
            engine = row['engine']
            major_version = row['major_version'] + '.' + row['minor_version'] if engine == 'mysql' else row['major_version']
            
            # Fetch EOL data for the engine and major_version
            eol_date = get_eol_date(engine, major_version)
            if eol_date:
                # Append EOL date to the DataFrame for MySQL instances
                rds_data.at[index, 'eol_date'] = eol_date

        # Calculate date difference between today and eol_date
        rds_data['eol_date'] = pd.to_datetime(rds_data['eol_date'])
        rds_data['days_until_eol'] = (rds_data['eol_date'] - datetime.now()).dt.days

        # Calculate years and months until EOL
        rds_data['years_months_until_eol'] = rds_data['eol_date'].apply(lambda x: calculate_years_months_diff(x))

        # Filter rows where date difference is less than 3 days but greater than 0 days
        expiring_soon = rds_data[(rds_data['days_until_eol'] < 365) & (rds_data['days_until_eol'] > 0)]

        # Send notification for each expiring instance
        for index, row in expiring_soon.iterrows():
            message = f"Dear {row['primary_owner']},\n\n"
            message += f"This is to inform you that the AWS service: {row['engine']} with version {row['major_version']} is expiring soon in {row['years_months_until_eol']}.\n\n"
            message += f"Thanks and Regards \nAWS Admin team"
            send_notification(message)

        # Write the updated DataFrame back to a new CSV file in memory
        csv_buffer = io.StringIO()
        rds_data.to_csv(csv_buffer, index=False)

        # Upload the processed CSV file to S3
        s3.put_object(Bucket=bucket_name, Key='RDS/processed_rds.csv', Body=csv_buffer.getvalue())

        return {
            'statusCode': 200,
            'body': 'Processed file has been analyzed and uploaded to S3.'
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'An error occurred while processing the file.'
        }
