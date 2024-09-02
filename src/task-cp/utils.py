import json
import requests
import logging
import polars as pl
import itertools
import os
import mlflow
import hashlib
from config import *
from flask import jsonify
from log import *

def evaluate_rule(config):
    logger = get_logger_with_task_code()
    feature_ = config['feature']
    type_ = config['type']
    condition_ = config['condition']
    value_ = config['value']
    
    type_ = type_.lower()
    # logger.info(f"type: {type_}")
    if type_ == 'string':
        if condition_ == "包含":
            return pl.col(feature_).str.contains(value_)
        elif condition_ == "不包含":
            return ~pl.col(feature_).str.contains(value_)
        elif condition_ == "等于":
            return pl.col(feature_) == value_
    elif type_ == 'int' or type_ == 'long':
        value_ = int(value_)
    elif type_ in ['float'] or type_ == 'double':
        value_ = float(value_)
    elif type_ == 'date':
        pass
        # value_ = pl.lit(pl.date(value_))
    elif type_ == 'bool':
        value_ = (value_.lower() == 'true')
    else:
        raise ValueError(f"Unknown value type: {type_}. Case-insensitive, Supported types: string, int, long, float, double, date, bool")

    if condition_ == '=': condition_ = '=='
    expression = f"pl.col('{feature_}') {condition_} value_"
    return eval(expression)

def compute_fun(dataset, feature_list, alg_list):
    logger = get_logger_with_task_code()
    result = {}
    try:
        for index, feature in enumerate(feature_list):
            result_of_feature = {}
            for alg in alg_list[index]:
                if alg == "max":
                    value = dataset[feature].max()
                elif alg == "min":
                    value = dataset[feature].min()
                elif alg == "var":
                    value = dataset[feature].var()
                elif alg == "mean":
                    value = dataset[feature].mean()
                elif alg == "std":
                    value = dataset[feature].std()
                else :
                    raise Exception("Not support this alg : {}".format(alg))
                result_of_feature[alg] = value
            result[feature] = result_of_feature
    except Exception as e:
        logger.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    return result

def reply_callback(data, request_json, error_msg = "", callback = True):
    # set_task_code('NEW_TASK_CODE')
    task_code = request_json.get("task_code")
    logger = get_logger_with_task_code()
    
    service_type = request_json.get("service_type")
    callback_url = request_json.get("callback_url")
    dataset_code = ""

    message = error_msg
    part_data = ""
    status = "failed"
    part_result_file = ''
    full_result_file = ''
    result_count = 0
    full_file_sizes = 0
    is_error = True
    part_num = 0

    try:
        if not data:
            is_error = False
            if service_type == "PIR":
                part_data = []
            raise ValueError("Result is empty")
        
        if service_type == "PSI":
            condition_configs = request_json.get("conditionConfigs")
            if condition_configs and len(condition_configs) > 0 and "dataset_code" in condition_configs[0]:
                dataset_code = condition_configs[0]["dataset_code"]
            part_num = request_json.get("result_config")["amount"]
        elif service_type == "STAT":
            conditional_config = request_json.get("conditional_config")
            if conditional_config and len(conditional_config) > 0 and "dataset_code" in conditional_config[0]:
                dataset_code = conditional_config[0]["dataset_code"]
        elif service_type == "PIR":
            if "dataset_code" in request_json:
                dataset_code = request_json["dataset_code"]
        else:
            raise ValueError("Unknown type : ", service_type)

        message = "Processing " + service_type + " task successfully"

        if isinstance(data, list):
            result_count = len(data[0])
            if isinstance(data[0], (list, tuple, str)):
                print("lktest, ", part_num)
                part_data = [item[:Config.RESULT_NUM_LIMIT] for item in data]
            elif isinstance(data[0], dict):
                part_data = data[:Config.RESULT_NUM_LIMIT]
            else:
                part_data = data
        elif isinstance(data, dict):
            result_count = len(data)
            part_data = json.dumps(dict(itertools.islice(data.items(), Config.RESULT_NUM_LIMIT)))
        elif isinstance(data, pl.DataFrame):
            pass
        else:
            message = "error task type"
            status = "failed"
        part_path = "data/" + "part_result_" + task_code + ".txt"
        part_file = os.path.basename(part_path)
        if not os.path.exists("data"):
            os.makedirs("data")
        part_data_str = json.dumps(part_data, ensure_ascii=False)
        with open(part_path, "w", encoding="utf-8") as file:
            file.write(part_data_str)
        
        with mlflow.start_run(run_name=task_code, experiment_id=Config.EXPERIMENT_ID, nested=True) as run:
            mlflow.log_artifact(part_path)
            if Config.MLFLOW_TRACKING_URI is not None:
                part_result_file = f'{Config.MLFLOW_TRACKING_URI}/get-artifact?path={part_file}&run_uuid={run.info.run_id}'
            else:
                part_result_file = 'mlflow_addr not set'
            
        full_data = json.dumps(data)
        full_path = "data/" + "full_result_" + task_code + ".txt"
        full_file = os.path.basename(full_path)
        if not os.path.exists("data"):
            os.makedirs("data")
        with open(full_path, "w", encoding="utf-8") as file:
            file.write(full_data)
        full_file_sizes = os.path.getsize(full_path)
        with mlflow.start_run(run_name=task_code, experiment_id=Config.EXPERIMENT_ID, nested=True) as run:
            mlflow.log_artifact(full_path)
            if Config.MLFLOW_TRACKING_URI is not None:
                full_result_file = f'{Config.MLFLOW_TRACKING_URI}/get-artifact?path={full_file}&run_uuid={run.info.run_id}'
            else:
                full_result_file = 'mlflow_addr not set'
        status = "success"
        
    except Exception as e:
        if is_error:
            logger.error(str(e), exc_info=True)
        if error_msg:
            message = error_msg
        else:
            message = str(e)
        if not data:
            status = "success"
        else:
            status = "failed"
        
    callback_data = {
        "task_code": task_code,
        "service_type": service_type,
        "dataset_code": dataset_code,
        "response": {
            "message": message,
            "part_result": part_data,
            "status": status,
            "part_result_file": part_result_file,
            "full_result_file": full_result_file,
            "result_count": result_count,
            "full_file_sizes": full_file_sizes
        },
        
    }

    logger.info(f"callback_url: {callback_url}")
    logger.info(f"callback data: {callback_data}")
    
    if callback :
        headers = {'Content-Type': 'application/json'}
        response = requests.post(callback_url, headers=headers, json=callback_data, verify=False)
        # logger.info(f"response : {response}")
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text}")
    else:
        return callback_data
    
def hash_string(data):
    hash = hashlib.sha1()
    hash.update(data.encode('utf-8'))
    return hash.hexdigest()

def hash_buffer(data):
    hash = hashlib.sha1(data)
    return hash.hexdigest()