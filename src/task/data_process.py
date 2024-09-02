
import requests
from io import StringIO, BytesIO
import logging
import polars as pl
import json
import boto3
import threading
import utils
from flask import jsonify
from config import *
from log import *

# 文件流读取数据
def load_dataset_handler(request):
    try:
        global g_dataset_map
        file = request.files['data']
        file_content = file.read()
       
        # dataset_code = str(uuid.uuid4())
        dataset_code = utils.hash_buffer(file_content)
        # dataset = pl.read_csv(StringIO(file.read().decode('utf-8')), ignore_errors=True)
        dataset = pl.read_csv(StringIO(file_content.decode('utf-8')), ignore_errors=True)
        
        lock = threading.Lock()
        with lock:
            g_dataset_map[dataset_code] = dataset
        response = {
            "data": {"dataset_code": dataset_code},
            "msg": "Dataset loaded successfully",
            "code": 200
        }
        return jsonify(response)
    except Exception as e:
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    
# 文件链接读取数据
def task_download_file(request_json, dataset_code):
    print("start download")
    global g_dataset_map  
    try:
        
        task_code = request_json.get("serving_task_code")
        set_task_code(task_code)
        logger = get_logger_with_task_code()
    
        file_url = request_json.get('dataset_download_link')
        call_back_url = request_json.get("call_back_url")
        serving_task_code = request_json.get("serving_task_code")
        dataset_id = request_json.get("dataset_id")
        store_file = request_json.get("store_file")
        message = ""

        response = requests.get(file_url, verify=False)
        content_type = response.headers.get('Content-Type')
        dataset = ""
        
        print(response.status_code)
        state_code = response.status_code
        if state_code == 200:
            csv_data = response.text
            dataset = pl.read_csv(StringIO(csv_data))
            if store_file : 
                file_path = Config.FILE_PATH + dataset_code
                dataset.write_csv(file_path)
            if isinstance(dataset, pl.DataFrame):
                g_dataset_map[dataset_code] = dataset
                message = "get file success"
                logger.info(f"{message} : {dataset_code}")
                print(dataset)
            else:
                message = "get file failed"
        elif content_type == 'application/json':
            message = response.json()
            print(message)
            logger.info(f"get file url response : {message}")
        else:
            message = "get file failed"
            logger.error(f"{dataset_code} download failed, {response}")
            
        print(dataset)
    except Exception as e:
        state_code = 600
        message = str(e)
        print(message)
        logger.error(message, exc_info=True)
    callback_data = {
        "state_code" : state_code,
        "serving_task_code": serving_task_code,
        "dataset_id": dataset_id,
        "dataset_code": dataset_code,
        "message": message,
    }
    headers = {'Content-Type': 'application/json'}
    replay_response = requests.post(call_back_url, headers=headers, json=callback_data, verify=False)
    logger.info(replay_response)

# 加载本地文件或者s3_info读取数据
def local_file_load(request):
    global g_dataset_map  
    try:
        file_name = request.json.get('filename')
        # dataset_code = str(uuid.uuid4())
        dataset_code = utils.hash_string(file_name)
        dataset = None

        if "s3Info" in file_name:
            s3_data = json.loads(file_name)
            s3_info = s3_data['s3Info']
            s3 = boto3.client(
                's3',
                endpoint_url=s3_info["endpoint"],
                aws_access_key_id=s3_info["accessKey"],
                aws_secret_access_key=s3_info["secretKey"]
            )
            file_name = s3_info["fileName"]
            obj = s3.get_object(Bucket=s3_info["bucketName"], Key=file_name)
            dataset = pl.read_csv(BytesIO(obj['Body'].read()))
        else:
            dataset = pl.read_csv(file_name)
        lock = threading.Lock()
        print(dataset)
        with lock:
            g_dataset_map[dataset_code] = dataset
        response = {
            "data": {"dataset_code": dataset_code},
            "msg": "Dataset loaded successfully",
            "code": 200
        }
        return jsonify(response)
    except Exception as e:
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# 获得数据处理结果文件
def get_result_file_handler(request):
    try:
        global g_dataset_map
        dataset_code = request.json.get('dataset_code')
        dataset = g_dataset_map.pop(dataset_code, None)
        if dataset is None:
            raise ValueError(f"No dataset found for code: {dataset_code}")
        file_name = "processed_" + dataset_code + ".csv"
        output_path = Config.PRE_DEAL_PATH
        os.makedirs(output_path, exist_ok=True)
        output_file_path = os.path.join(output_path, file_name)
        dataset.write_csv(output_file_path)
        msg = "Pre deal data successfully"
    except Exception as e:
        msg = str(e)
        logging.error(msg, exc_info=True)
    return jsonify({"status": "success", "message": msg, "data_result": output_file_path})
