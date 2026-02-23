import json
from tempfile import TemporaryDirectory
import imaplib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from traceback import format_exc

import boto3
import pickle
import glob
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional, Union

from const import DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS
from email_processing import read_eml_from_dict
from pymilvus import MilvusClient
import urllib.request
import time
import base64
import requests

# TODO add retry - state machine


dynamodb = boto3.resource('dynamodb')

def download_message(access_token, message_id):
    """Get full message content using Gmail API and return extracted data."""
    try:
        url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}?format=full"
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {access_token}"}
        )

        with urllib.request.urlopen(req) as response:
            message = json.loads(response.read().decode())

        # Extract and return the email data immediately
        return extract_email_data(message)

    except Exception as e:
        print(f"Error getting message {message_id}: {e}")
        print(format_exc())
        return None


#
# def get_message_body(message):
#     import base64
#
#     def get_email_body(message):
#         payload = message['payload']
#
#         # Check if it's multipart
#         if 'parts' in payload:
#             # Multipart message - iterate through parts
#             for part in payload['parts']:
#                 mime_type = part['mimeType']
#
#                 # Get plain text or HTML
#                 if mime_type == 'text/plain':
#                     data = part['body']['data']
#                     return base64.urlsafe_b64decode(data).decode('utf-8')
#         else:
#             # Simple message - body is directly in payload
#             if 'data' in payload['body']:
#                 data = payload['body']['data']
#                 return base64.urlsafe_b64decode(data).decode('utf-8')
#
#         return None

def get_message_body(message):
    payload = message.get('payload')

    payload_body = payload.get('body')

    try:
        if payload_body.get("size") != 0:
            base64_body = payload_body.get('data')
            return base64.b64decode(base64_body).decode('utf-8', errors='ignore')
        else:
            parts = payload.get('parts')

            for part in parts:
                if part.get('mimeType') == 'text/plain':
                    part_body = part.get('body')
                    base64_body = part_body.get('data')
                    return base64.urlsafe_b64decode(base64_body).decode('utf-8', errors='ignore')
    except Exception as e:
        print(e)



# def get_message_body(message):
#     """Extract body from message."""
#
#     text_part = message['payload']['parts'][0]
#     text_data = text_part['body']['data']
#     text_content = base64.urlsafe_b64decode(text_data).decode('utf-8')
#
#     def get_body_recursive(payload):
#         if 'body' in payload and payload['body'].get('data'):
#             return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
#
#         if 'parts' in payload:
#             for part in payload['parts']:
#                 if part.get('mimeType') == 'text/plain':
#                     if part.get('body', {}).get('data'):
#                         return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
#
#             # If no text/plain, try first part
#             for part in payload['parts']:
#                 body = get_body_recursive(part)
#                 if body:
#                     return body
#
#         return None
#
#     return get_body_recursive(message.get('payload', {}))


def extract_email_data(message):
    """Extract useful data from Gmail API message."""
    headers = {h['name']: h['value'] for h in message.get('payload', {}).get('headers', [])}

    body = get_message_body(message)

    return {
        'id': message.get('id'),
        'thread_id': message.get('threadId'),
        'from': headers.get('From', 'N/A'),
        'to': headers.get('To', 'N/A'),
        'subject': headers.get('Subject', 'N/A'),
        'date': headers.get('Date', 'N/A'),
        'body': body
    }


def embed_worker(
        sample_model_input: Dict,
        data_link: Dict,
        model_id: str = "amazon.titan-embed-text-v2:0",
        accept: str = "application/json",
        content_type: str = "application/json",
) -> Dict:
    bedrock_runtime = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-east-1',
    )

    body = json.dumps(sample_model_input)

    # invoke model
    response = bedrock_runtime.invoke_model(body=body, modelId=model_id, accept=accept,
                                            contentType=content_type)

    response_body = json.loads(response.get('body').read())
    embedding = response_body.get("embedding")
    data_link['vector'] = embedding
    return data_link

def get_access_token(cognito_id: str, refresh: bool):
    kms_client = boto3.client('kms', region_name='us-east-1')
    table = dynamodb.Table('email_tokens')

    response = table.get_item(Key={'cognito_id': cognito_id})

    encrypted_access_token = response['Item'].get('encrypted_access_token')

    if refresh:
        encrypted_refresh_token = response['Item'].get('encrypted_refresh_token')
        ciphertext_blob = base64.b64decode(encrypted_refresh_token)

        response = kms_client.decrypt(
            CiphertextBlob=ciphertext_blob
        )
        decrypted_token = response['Plaintext'].decode('utf-8')

        token_url = "https://oauth2.googleapis.com/token"

        gmail_client_id = os.environ.get('GMAIL_CLIENT_ID')
        gmail_client_secret = os.environ.get('GMAIL_CLIENT_SECRET')

        data = {
            "grant_type": "refresh_token",
            "refresh_token": decrypted_token,
            "client_id": gmail_client_id,
            "client_secret": gmail_client_secret
        }

        response = requests.post(token_url, data=data)
        tokens = response.json()
        access_token = tokens.get('access_token')
    else:
        ciphertext_blob = base64.b64decode(encrypted_access_token)

        response = kms_client.decrypt(
            CiphertextBlob=ciphertext_blob
        )
        access_token = response['Plaintext'].decode('utf-8')

    return access_token


def lambda_handler(event, context):
    """
    TODO add description
    """
    start = time.time()
    print("Received event:", event)

    user = event.get("user")
    cognito_id = event.get("cognito_id")
    gmail_folder_name = event.get("gmail_folder")
    uids_to_download = event.get("data")
    max_workers = event.get("num_workers", 4)
    i = event.get("batch_number")
    run_id = event.get("run_id")

    status_table = dynamodb.Table("email_download_status")
    status = {
        "run_id": run_id, "worker_id": str(i), "status": "Incomplete", "timeout": int(time.time()) + DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS
    }
    status_table.put_item(Item=status)

    access_token = get_access_token(cognito_id, True)

    print(user)
    print("folder:", gmail_folder_name)
    print(uids_to_download)
    print(f"{max_workers=}")
    print("index:", i)

    # Download all emails and collect results (avoiding race conditions)
    emails_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_message, access_token=access_token, message_id=uid) for uid in
                   uids_to_download]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:  # Only add successful downloads
                    emails_data.append(result)
            except Exception as e:
                print(f"Error processing future: {e}")

    print(f"Successfully downloaded {len(emails_data)} emails.")

    # Process the email data - convert dicts to Eml objects and cleanse
    processed_emails = []
    for email_dict in emails_data:
        try:
            mail = read_eml_from_dict(email_dict)
            mail.cleanse_eml()
            processed_emails.append(mail)
        except Exception as e:
            print(f"Error processing email {email_dict.get('id', 'unknown')}: {e}")

    client = MilvusClient(uri="https://in03-349aeff0ec8bf13.serverless.gcp-us-west1.cloud.zilliz.com",
                          token="1d97f811965d4488e85de0510f459af6b5842dcd5c6faff395afb4a246cbaafe39657a0b7956e66410215c971394586d60f81704")

    client.describe_collection(collection_name="Email_RAG") # TODO collection name based on user?

    to_insert = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for eml in processed_emails:
            eml_pieces = []
            j = 0
            while j < len(eml.text):
                eml_pieces.append(eml.text[j:j + 20000])
                j += 20000

            for piece in eml_pieces:
                sample_model_input = {
                    "inputText": piece,
                    "dimensions": 1024,
                    "normalize": True
                }
                data_link = {
                    'primary_key': eml.date,  ## TODO hash function based on content and time
                    'subject': eml.subject,
                    'recipients': eml.receiver,
                    'sender': eml.sender,
                    'date': eml.date,
                    # list of reply times / timepoints - if convo chain
                    'part': j,
                    'total_parts': len(eml_pieces),
                    'raw_text': eml.text,
                }

                futures.append(
                    executor.submit(embed_worker,
                                    sample_model_input=sample_model_input,
                                    data_link=data_link))

        for future in as_completed(futures):
            try:
                result = future.result()
                to_insert.append(result)
            except Exception as e:
                print(f"Error embedding: {e}")

    client.insert(
        collection_name="amazon_collection",
        data=to_insert
    )


    # update completion status
    status_table.update_item(
        Key={"run_id": run_id, "worker_id": str(i)},
        UpdateExpression="SET #s = :complete",
        ExpressionAttributeNames={
            "#s": "status"
        },
        ExpressionAttributeValues={
            ":complete": "Complete",
        }
    )

    print(f"took {time.time() - start} ms")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "success!",
        }),
    }


if __name__ == "__main__":
    # event = {
    #     "user": "jz5822.nyu@gmail.com",
    #     "cognito_id": "34c85458-40d1-7074-2b96-e8ff166608ee",
    #     "data": ["1879ac5e4ba4ce09", "1879aa4267c40949", "1879a978d876149d", "1879a8e8cbc2650c", "1879a8b48f0ad181"],
    #     "batch_number": 5,
    #     "run_id": "abcdefghi"
    # }

    # event = {
    #     "user": "silversnowblossom14@gmail.com",
    #     "cognito_id": "a4281448-90b1-7086-0898-910fbe596b29",
    #     "data": ["19bac9d127561cf0", "19a714a257d484f8", "1983cf4a13fa29ee", "19419c37121332af", "18f90a151f0d18bf"],
    #     "batch_number": 5,
    #     "run_id": "abcdefghijk"
    # }

    event = {'user': 'silversnowblossom14@gmail.com',
             'cognito_id': 'a4281448-90b1-7086-0898-910fbe596b29',
             'gmail_folder': 'INBOX',
             'data': ['199e75e490c0f59e', '199e3baefd4eb537', '199c0951c0bfed85', '199c08591f9e7a89', '199c084ca33fb35f', '199bea61363b7424', '199afd1145ed3d99', '199a691387056bff', '19997b571958c584', '19995b742451278b', '1992d39ea0473564', '198eb5ffb1707fd6', '198b59b24ed9dca7', '198b59b158e85d1f', '198b59ad088d474b', '1987df3e158ec0f4', '1986f7a281dfa37b', '198674e1db1cc355', '1985ea919cfe7690', '1985ea916fd1da9b', '1985ea90c807d191', '1985ea9014d7f534', '19855b41de762386', '1984b7141af93a51', '19846413a15e26d5', '19844e36f282a36f', '1984444a2f7f5159', '19843e0321b9d1a3', '198421c280aac530', '1984119922b38e65', '1983ff97e03d9de8', '1983dd259cdc2c12', '1983d6c17e4f8403', '1983cf4a13fa29ee', '1983cf4a12fc4cd9', '1983ca18fd3c94ec', '1983c98e529568ae'],
             'index': 1, 'num_workers': 1, 'run_id': '20260216-163419-37587fb3'}

    print(lambda_handler(event, None))