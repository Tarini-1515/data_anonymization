from flask import Flask, jsonify, request, g, Response
from boto3 import client
import boto3
import pandas as pd
import cryptpandas as crp
import sys
import time
import math
import os, psutil
import json
import botocore

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

app = Flask(__name__)

@app.before_request
def before_request():
    g.start = time.time()

@app.after_request
def after_request(response):
    diff = time.time() - g.start

    if ((response.response)):
        response.set_data(response.get_data().replace(b'__EXECUTION_TIME__', bytes(str(round(diff * 1000)) + ' ms', 'utf-8')))
    return response

@app.route('/', methods=['GET'])
def hello():
    process = psutil.Process(os.getpid())

    obj = {
        "data": "Welcome to Data Anonymization",
        "memoryUsed": convert_size(process.memory_info().rss),
        "executionTime": "__EXECUTION_TIME__"
    }
    return jsonify(obj)

@app.route('/data', methods=['POST'])
def fetchDataAsJson():
    bucket_name = request.json['bucket_name']
    bucket_key = request.json['bucket_key']

    conn = client('s3')

    obj = conn.get_object(
        Bucket = bucket_name,
        Key = bucket_key
    )
    body = obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))

    process = psutil.Process(os.getpid())

    obj = {
        "data": df.to_json(orient="records"),
        "memoryUsed": convert_size(process.memory_info().rss),
        "executionTime": "__EXECUTION_TIME__"
    }
    return  jsonify(obj)

@app.route('/data/split', methods=['POST'])
def splitData():
    bucket_name = request.json['bucket_name']
    bucket_key = request.json['bucket_key']
    output_key_personal = request.json.get('output_key_personal', None)
    output_key_personal_encrypt = request.json.get('output_key_personal_encrypt', None)
    local_key_personal_encrypt = 'encrypted_output/personal_detail.crypt'
    local_key_personal_encrypt_pwd = 'dfpd'
    output_key_medical = request.json.get('output_key_medical', None)
    output_key_medical_encrypt = request.json.get('output_key_medical_encrypt', None)
    local_key_medical_encrypt = 'encrypted_output/medical_detail.crypt'
    local_key_medical_encrypt_pwd = 'mrpd'

    conn = client('s3')
    s3_resource = boto3.resource('s3')

    obj = conn.get_object(
        Bucket = bucket_name,
        Key = bucket_key
    )
    body = obj['Body']
    csv_string = body.read().decode('utf-8')

    # Load test CSV
    df = pd.read_csv(StringIO(csv_string))

    # Add Patient ID column
    df.insert(0, 'patient_id', range(0, 0 + len(df)))

    # Define whitelist
    personal_record_col_wl = ["patient_id", "medical_id", "age", "sex", "children", "region", "charges", "first_name", "last_name", "email", "phone_number", "religion", "race"]
    medical_record_col_wl = ["patient_id", "medical_id", "age", "bmi", "smoker", "charges"]

    # Get columns name
    columns_name = df.columns.values

    # Initiate blacklist
    personal_record_col_bl = []
    medical_record_col_bl = []

    # Filtering unexpected columns based on whitelist
    for val in columns_name:
        if (val.lower() not in personal_record_col_wl):
            personal_record_col_bl.append(val)

        if (val.lower() not in medical_record_col_wl):
            medical_record_col_bl.append(val)

    # Duplicate, export to different CSV and encrypt it
    if output_key_personal:
        df_personal_record = df.copy()
        df_personal_record.drop(personal_record_col_bl, axis = 1, inplace=True)
        df_personal_record_csv_buffer = StringIO()
        df_personal_record.to_csv(df_personal_record_csv_buffer, index=False)
        s3_resource.Object(bucket_name, output_key_personal).put(Body=df_personal_record_csv_buffer.getvalue())

    if output_key_medical_encrypt:
        crp.to_encrypted(df_personal_record, password=local_key_personal_encrypt_pwd, path=local_key_personal_encrypt)
        s3_resource.Bucket(bucket_name).upload_file(local_key_medical_encrypt, output_key_medical_encrypt)
    

    if output_key_medical:
        df_medical_record = df.copy()
        df_medical_record.drop(medical_record_col_bl, axis = 1, inplace=True)
        df_medical_record_csv_buffer = StringIO()
        df_medical_record.to_csv(df_medical_record_csv_buffer, index=False)
        s3_resource.Object(bucket_name, output_key_medical).put(Body=df_medical_record_csv_buffer.getvalue())

    if output_key_medical_encrypt:
        crp.to_encrypted(df_medical_record, password=local_key_medical_encrypt_pwd, path=local_key_medical_encrypt)
        s3_resource.Bucket(bucket_name).upload_file(local_key_medical_encrypt, output_key_medical_encrypt)

    process = psutil.Process(os.getpid())

    data = {
        "output_key_personal": output_key_personal,
        "output_key_personal_encrypt": output_key_personal_encrypt,
        "output_key_medical": output_key_medical,
        "output_key_medical_encrypt": output_key_medical_encrypt,
        "memoryUsed": convert_size(process.memory_info().rss),
        "executionTime": "__EXECUTION_TIME__"
    }

    if not output_key_personal:
        data.pop("output_key_personal")

    if not output_key_personal_encrypt:
        data.pop("output_key_personal_encrypt")

    if not output_key_medical:
        data.pop("output_key_medical")

    if not output_key_medical_encrypt:
        data.pop("output_key_medical_encrypt")

    return jsonify(data)

@app.route('/data/calculate', methods=['POST'])
def calculateData():
    bucket_name = request.json['bucket_name']
    bucket_key = request.json['bucket_key']
    is_encrypt = request.json['is_encrypt']

    local_data_decrypt_crypt = 'decrypted_input/data.crypt'
    good_health_count = 0
    below_average_health_count = 0
    bad_health_count = 0

    conn = client('s3')
    s3_resource = boto3.resource('s3')

    try:
        s3_resource.Object(bucket_name, bucket_key).load()
    except botocore.exceptions.ClientError as e:
        print(e)
        process = psutil.Process(os.getpid())

        data = {
            "data": "File does not exist.",
            "memoryUsed": convert_size(process.memory_info().rss),
            "executionTime": "__EXECUTION_TIME__"
        }
        return jsonify(data)
    else:
        if is_encrypt:
            conn.download_file(bucket_name, bucket_key, local_data_decrypt_crypt)
            df = crp.read_encrypted(path=local_data_decrypt_crypt, password='mrpd')
        else:
            obj = conn.get_object(
                Bucket = bucket_name,
                Key = bucket_key
            )
            body = obj['Body']
            csv_string = body.read().decode('utf-8')

            # Load test CSV
            df = pd.read_csv(StringIO(csv_string))

            columns_name = df.columns.values

            medical_record_col_wl = ["patient_id", "medical_id", "age", "bmi", "smoker", "charges"]

            medical_record_col_bl = []

            for val in columns_name:
                if (val.lower() not in medical_record_col_wl):
                    medical_record_col_bl.append(val)

            df_medical_record = df.copy()
            df_medical_record.drop(medical_record_col_bl, axis = 1, inplace=True)

    # Perform calculation
    good_health_count = df.query("bmi < 30 & smoker == 'no'").shape[0]
    below_average_health_count = df.query("bmi < 30 & smoker == 'yes'").shape[0]
    bad_health_count = df.query("bmi >= 30 & smoker == 'yes'").shape[0]

    process = psutil.Process(os.getpid())

    data = {
        "goodHealth": good_health_count,
        "belowAverageHealth": below_average_health_count,
        "badHealth": bad_health_count,
        "memoryUsed": convert_size(process.memory_info().rss),
        "executionTime": "__EXECUTION_TIME__"
    }
    return jsonify(data)

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024) ))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


if __name__ == '__main__':
	app.run(debug=True)


