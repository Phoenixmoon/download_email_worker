import json
from tempfile import TemporaryDirectory
import imaplib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import pickle
import glob
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional, Union
from email_processing import read_eml
from pymilvus import MilvusClient
import time

# import requests


def download_message(svr, uid, file):
    resp, lst = svr.uid('fetch', str(uid), '(RFC822)')
    if resp != 'OK' or not lst or not lst[0]:
        raise Exception(f"Failed to fetch UID {uid}: {resp}, {lst}")
    with open(file, 'wb') as f:
        f.write(lst[0][1])


# def download_email(uid, gmail_folder_name, user, pwd):
#     svr = connect_to_server(user, pwd)
#     svr.select(gmail_folder_name, readonly=True)
#     print(f"Downloading UID: {uid}")
#     filepath = f"{uid}.eml"
#     download_message(svr, uid, filepath)
#     svr.logout()

def download_email(uid, gmail_folder_name, user, pwd):
    t0 = time.time()
    print(f"Downloading UID: {uid}")
    svr = connect_to_server(user, pwd)
    t1 = time.time()
    svr.select(gmail_folder_name, readonly=True)
    filepath = f"{uid}.eml"
    download_message(svr, uid, filepath)
    svr.logout()
    print(f'Downloaded {uid}, took {round(time.time()-t1, 2)}s to download & server connection took {round(t1 - t0,2)}')



def connect_to_server(user, pwd):
    svr = imaplib.IMAP4_SSL('imap.gmail.com')
    svr.login(user, pwd)
    return svr


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


def lambda_handler(event, context):
    """
    TODO add description
    """

    user = event.get("user")
    pwd = event.get("pwd")
    gmail_folder_name = event.get("gmail_folder", 'INBOX')
    uids_to_download = event.get("data")
    max_workers = event.get("num_workers", 4)
    i = event.get("index")

    print(user)
    print(gmail_folder_name)
    print(uids_to_download)
    print(f"{max_workers=}")
    print("index:", i)

    with TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_email, uid=uid, gmail_folder_name=gmail_folder_name, user=user,
                       pwd=pwd) for uid in uids_to_download]

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    print(f"Error downloading UID {future}: {e}")

        print("All emails downloaded.")

        emails = glob.glob("*.eml")

        processed_emls = []
        for eml in emails:
            try:
                mail = read_eml(eml)
                mail.cleanse_eml()
                processed_emls.append(mail)
            except:
                print("current eml:", eml)


        client = MilvusClient(uri="https://in03-349aeff0ec8bf13.serverless.gcp-us-west1.cloud.zilliz.com",
                              token="1d97f811965d4488e85de0510f459af6b5842dcd5c6faff395afb4a246cbaafe39657a0b7956e66410215c971394586d60f81704")

        client.describe_collection(collection_name="Email_RAG")

        ##TODO collection name depends on user

        to_insert = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for eml in processed_emls:
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
                        'primary_key': eml.date, ## TODO hash function based on content and time
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
                    print(f"Error downloading UID {future}: {e}")

        client.insert(
            collection_name="amazon_collection",
            data=to_insert
        )


        # with open('emails_lst.pkl', 'wb') as f:
        #     pickle.dump(processed_emls, f)
        #
        # time = datetime.now().strftime('%Y-%m-%d_%H%M%S%f')
        # s3_key = f"{user}/{time}_{i}.pkl"
        # s3 = boto3.client('s3')
        # s3.upload_file('emails_lst.pkl', 'rag-email', s3_key)
        #
        # print("succesfully upload to s3")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "success!",
        }),
    }


if __name__ == "__main__":
    event = {
        "user": "jz5822.nyu@gmail.com",
        "pwd": "zkcj tcwx uaud tdbi",
        "data": [10, 11, 12, 13, 14, 15, 16],
        "index": 1
    }

    print(lambda_handler(event, None))
