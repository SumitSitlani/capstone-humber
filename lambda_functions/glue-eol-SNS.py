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

# Function to make API call and extract EOL date
def get_eol_date(glue_version):
    try:
        # Format version number to include decimal point if missing
        glue_version = f"{float(glue_version):.1f}"
        
        # Check if the response is already cached
        if glue_version in api_cache:
            return api_cache[glue_version]

        # Construct API URL
        url = f"https://endoflife.date/api/amazon-glue/{glue_version}.json"
        response = requests.get(url)
        
        # Handle API response
        if response.status_code == 200:
            eol_data = response.json()
            eol_date_str = eol_data.get("eol")
            if eol_date_str == "false":
                api_cache[glue_version] = "Not End-of-Life"  # Cache "Not End-of-Life" for "false" response
                return "Not End-of-Life"
            elif eol_date_str:
                eol_date = datetime.strptime(eol_date_str, "%Y-%m-%d")
                api_cache[glue_version] = eol_date  # Cache the response
                return eol_date
            else:
                api_cache[glue_version] = None  # Cache "None" if date is not available
                return None
        else:
            api_cache[glue_version] = None  # Cache "None" for API errors
            return None
    except Exception as e:
        print(f"Error processing version {glue_version}: {str(e)}")
        api_cache[glue_version] = None  # Cache "None" for any processing errors
        return None

# Function to send notification
def send_notification(message):
    try:
        # Publish message to SNS topic
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:210929744970:Capstone-EOL-SNS',
            Message=message,
            Subject="AWS Glue Expiration Notification"
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
        file_key = 'glue.csv'
        
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))

        # Apply get_eol_date function to each row and create a new column "EOL Date"
        df["EOL Date"] = df["glue_version"].apply(get_eol_date)

        # Convert "EOL Date" column to datetime
        df["EOL Date"] = pd.to_datetime(df["EOL Date"])

        # Calculate date difference between today and EOL date
        df['days_until_eol'] = (df['EOL Date'] - datetime.now()).dt.days

        # Calculate years and months until EOL
        df['years_months_until_eol'] = df['EOL Date'].apply(lambda x: calculate_years_months_diff(x))

        # Filter rows where date difference is less than or equal to 365 days
        expiring_soon = df[(df['days_until_eol'] < 365) & (df['days_until_eol'] > 0)]

        # Send notification for each expiring instance
        for index, row in expiring_soon.iterrows():
            message = f"Dear {row['primary_owner']},\n\n"
            message += f"This is to inform you that AWS Glue version {row['glue_version']} is expiring soon in {row['years_months_until_eol']}.\n\n"
            message += f"Please take the required actions soon.\n\n"
            message += "Best regards,\nYour AWS Team"
            send_notification(message)
            print("Notification sent successfully.")

        # Write the updated DataFrame back to a new CSV file in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the processed CSV file to S3
        s3.put_object(Bucket=bucket_name, Key='Glue/processed_glue.csv', Body=csv_buffer.getvalue())
        
        return {
            'statusCode': 200,
            'body': 'Processed file has been uploaded to S3 successfully.'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'An error occurred while processing the file.'
        }
