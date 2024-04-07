import pandas as pd
import requests
import boto3
import io
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')
# Initialize SNS client
sns = boto3.client('sns')

# Dictionary to cache API responses
api_cache = {}

# Function to fetch EOL data from the API with caching
def get_eol_date(runtime, version):
    try:  
        # Check if the response is already cached
        if (runtime, version) in api_cache:
            return api_cache[(runtime, version)]

        # Construct API URL
        url = f"https://endoflife.date/api/{runtime}/{version}.json"
        response = requests.get(url)
        
        # Handle API response
        if response.status_code == 200:
            eol_date = response.json().get("support")
            # Cache the fetched data
            api_cache[(runtime, version)] = eol_date
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
            Subject="AWS Lambda Expiration Notification"
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
        file_key = 'lambda.csv'
        
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        lambda_data = pd.read_csv(io.BytesIO(response['Body'].read()))

        # Iterate over each row in the DataFrame
        for index, row in lambda_data.iterrows():
            runtime = row['runtime']
            if runtime.startswith('python'):
                # Split 'runtime' column into 'runtime' and 'version' for Python runtimes
                version_lambda = row['runtime'].split('python')
                lambda_data.at[index, 'runtime'] = 'python'
                if len(version_lambda) > 1:
                    lambda_data.at[index, 'version'] = version_lambda[1]
                else:
                    lambda_data.at[index, 'version'] = None
            elif runtime.startswith('nodejs'):
                # Split 'runtime' column into 'runtime' and 'version' for Node.js runtimes
                version_lambda = row['runtime'].split('js')
                lambda_data.at[index, 'runtime'] = 'nodejs'
                if len(version_lambda) > 1:
                    lambda_data.at[index, 'version'] = version_lambda[1].rstrip('.x')
                else:
                    lambda_data.at[index, 'version'] = None

        # Initialize 'eol_date' column with None for Lambda instances
        lambda_data['eol_date'] = None

        # Iterate over each row in the DataFrame for Lambda instances
        for index, row in lambda_data.iterrows():
            runtime = row['runtime']
            version = row['version']
            # Fetch EOL data for the runtime and version
            eol_date = get_eol_date(runtime, version)
            if eol_date:
                # Append EOL date to the DataFrame for Lambda instances
                lambda_data.at[index, 'eol_date'] = eol_date

        # Calculate date difference between today and eol_date
        lambda_data['eol_date'] = pd.to_datetime(lambda_data['eol_date'])
        lambda_data['days_until_eol'] = (lambda_data['eol_date'] - datetime.now()).dt.days

        # Calculate years and months until EOL
        lambda_data['years_months_until_eol'] = lambda_data['eol_date'].apply(lambda x: calculate_years_months_diff(x))

        # Filter rows where date difference is less than 90 days but greater than 0 days
        expiring_soon = lambda_data[(lambda_data['days_until_eol'] <= 365) & (lambda_data['days_until_eol'] > 0)]

        # Send notification for each expiring instance
        for index, row in expiring_soon.iterrows():
            message = f"Dear {row['primary_owner']},\n\n"
            message += f"This is to inform you that the AWS Lambda function with runtime {row['runtime']} and version {row['version']} is expiring soon in {row['years_months_until_eol']}.\n\n"
            message += f"Please take the required actions soon.\n\n"
            message += "Best regards,\nYour AWS Team"
            send_notification(message)

        # Write the updated DataFrame back to a new CSV file in memory
        csv_buffer = io.StringIO()
        lambda_data.to_csv(csv_buffer, index=False)

        # Upload the processed CSV file to S3
        s3.put_object(Bucket=bucket_name, Key='Lambda/processed_lambda.csv', Body=csv_buffer.getvalue())

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
