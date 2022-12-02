from flask import Flask, jsonify, request, g
from boto3 import client
import pandas as pd
import cryptpandas as crp
import sys
import time
import math

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

app = Flask(__name__)

@app.route('/hello', methods=['GET'])
def helloworld():
    conn = client('s3')

    for key in conn.list_objects(Bucket='21117314-data-anonymization')['Contents']:
        print(key['Key'])

    obj = conn.get_object(
        Bucket = '21117314-data-anonymization',
        Key = 'raws/raw_data2.csv'
    )
    body = obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))

    if (request.method == 'GET'):
        data = {
            "message": "Hello World",
            "memoryUsed": convert_size(sys.getsizeof(df)),
            "executionTime": "__EXECUTION_TIME__"
            }
        return jsonify(data)

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

@app.before_request
def before_request():
    g.start = time.time()

@app.after_request
def after_request(response):
    diff = time.time() - g.start
    print()
    if ((response.response)):
        response.set_data(response.get_data().replace(b'__EXECUTION_TIME__', bytes(str(round(diff * 1000)) + ' ms', 'utf-8')))
    return response


if __name__ == '__main__':
	app.run(debug=True)


