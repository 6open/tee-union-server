from flask import Flask, jsonify, request
import polars as pl
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData  
import uuid, json
import threading  
from polars import DataFrame  
from io import StringIO  
import logging
import time
import requests
import os
import mlflow
import itertools
from multiprocessing import Manager
from datetime import datetime
import boto3
from io import BytesIO

app = Flask(__name__)

# g_service_flag = "SERVER_1K"
g_service_flag = "PRE-DEAL_BP"
g_version = "1.0.21"
g_server_version = g_service_flag + ": " + g_version

g_dataset_map = {}

# mlflow_addr = "http://192.168.50.10:8999"
mlflow_addr = os.getenv('MLFLOW_TRACKING_URI')
experiment_name = 'tee-sql-service'
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

RESULT_NUM_LIMIT = 20           # 显示不超过该数量的结果


@app.route('/healthz')
def health_api():
    return 'OK'

@app.route('/version')
def get_version():
    print(g_server_version)
    response = {"msg" : g_server_version}
    return jsonify(response)

def reply_callback(data, request_json, error_msg=""):
    task_code = request_json.get("task_code")
    service_type = request_json.get("service_type")
    callback_url = request_json.get("callback_url")
    dataset_code = ""
    
    headers = {'Content-Type': 'application/json'}
    message = ""
    part_data = ""
    status = "failed"
    part_result_file = ''
    full_result_file = ''
    is_error = True
    try:
        if service_type == "PSI":
            condition_configs = request_json.get("conditionConfigs")
            if condition_configs and len(condition_configs) > 0 and "dataset_code" in condition_configs[0]:
                dataset_code = condition_configs[0]["dataset_code"]
        elif service_type == "STAT":
            conditional_config = request_json.get("conditional_config")
            if conditional_config and len(conditional_config) > 0 and "dataset_code" in conditional_config[0]:
                dataset_code = conditional_config[0]["dataset_code"]
        elif service_type == "PIR":
            if "dataset_code" in request_json:
                dataset_code = request_json["dataset_code"]
        else:
            raise ValueError("Unlnow type : ", service_type)
        
        message = "Processing " + service_type + " task successfully"
            
        
        if error_msg!="":
            print("lktsts")
            is_error = True
            raise ValueError(error_msg)
        if not data:
            is_error = False
            raise ValueError("Result is empty")
        if isinstance(data, list):  # psi pir
            if isinstance(data[0], (list, tuple, str)):
                # part_data.append(item[:RESULT_NUM_LIMIT])
                part_data = [item[:RESULT_NUM_LIMIT] for item in data]
            elif isinstance(data[0], dict):
                part_data = data[:RESULT_NUM_LIMIT]
            else:
                part_data = data

        elif isinstance(data, dict):    # stat
            print("字典")
            part_data = json.dumps(dict(itertools.islice(data.items(), RESULT_NUM_LIMIT)))
        elif isinstance(data, pl.DataFrame):
            print("dataframe")
        else:
            message = "error task type"
            status = "failed"
        print(part_data)
        part_path = "data/" + "part_result_" + task_code + ".txt"
        part_file = os.path.basename(part_path)
        if not os.path.exists("data"):
            os.makedirs("data")
        part_data_str = json.dumps(part_data, ensure_ascii=False)
        with open(part_path, "w", encoding="utf-8") as file:
            file.write(part_data_str)
        with mlflow.start_run(run_name=task_code, experiment_id=experiment_id) as run:
            mlflow.log_artifact(part_path)
            if mlflow_addr is not None:
                part_result_file = f'{mlflow_addr}/get-artifact?path={part_file}&run_uuid={run.info.run_id}'
            else:
                part_result_file = 'mlflow_addr not set'
            print(full_result_file)
            
        full_data = json.dumps(data)
        full_path = "data/" + "full_result_" + task_code + ".txt"
        full_file = os.path.basename(full_path)
        if not os.path.exists("data"):
            os.makedirs("data")
        with open(full_path, "w", encoding="utf-8") as file:
            file.write(full_data)
        with mlflow.start_run(run_name=task_code, experiment_id=experiment_id) as run:
            mlflow.log_artifact(full_path)
            if mlflow_addr is not None:
                full_result_file = f'{mlflow_addr}/get-artifact?path={full_file}&run_uuid={run.info.run_id}'
            else:
                full_result_file = 'mlflow_addr not set'
            print(full_result_file)
        status = "success"
        
    except Exception as e:
        print(str(e))
        if is_error:
            logging.error(str(e), exc_info=True)
            status = "failed"
        else:
            status = "success"
        if error_msg:
            message = error_msg
        else:
            message = str(e)
        
    callback_data = {
            "task_code" : task_code,
            "service_type" : service_type,
            "dataset_code" : dataset_code,
            "response": {
                "message": message,
                "part_result" : part_data,
                "status": status,
                "part_result_file" : part_result_file,
                "full_result_file" : full_result_file
            }
        }
    logging.info(f"callback_url: {callback_url}")
    logging.info(f"callback data: {callback_data}")
    response = requests.post(callback_url, headers=headers, json=callback_data, verify=False)
    logging.info(f"response : {response}")

# 通过文件名加载数据
@app.route('/tee/filename/load', methods=['POST'])
def load_filename():
    global g_dataset_map  
    try:
        dataset_code = str(uuid.uuid4())
        dataset = None
        data_info = request.json.get('filename')
        if 's3Info' not in data_info:
            file_name = data_info
            # file_name = os.path.join("/data/storage/dataset", file_name)
            dataset = pl.read_csv(file_name)
        else:
            s3_data = json.loads(data_info)
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
        lock = threading.Lock()  
        with lock:
            print("filename data: ", dataset)
            g_dataset_map[dataset_code] = (dataset, data_info)
        response = {  
            "data": {  
                "dataset_code": dataset_code  
            },  
            "msg": "Dataset loaded successfully",  
            "code": 200 
        }
        logging.info(f"filename: {file_name}")
        logging.info(f"dataset code: {dataset_code}")
        return jsonify(response)
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    
# 上传文件数据
@app.route('/tee/file/upload', methods=['POST'])
def load_dataset():
    file = request.files['data']  
    dataset = pl.read_csv(StringIO(file.read().decode('utf-8')), ignore_errors=True)  
    global g_dataset_map  
    try:
        dataset_code = str(uuid.uuid4())  
        lock = threading.Lock()  
        with lock:  
                g_dataset_map[dataset_code] = (dataset,"")
        response = {  
            "data": {  
                "dataset_code": dataset_code 
            },  
            "msg": "Dataset loaded successfully",  
            "code": 200  
        }  
        logging.info(f"dataset code: {dataset_code}")
        return jsonify(response)
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

def task_download_file(request_json, dataset_code):
    print("start download")
    global g_dataset_map  
    try:
        file_url = request_json.get('dataset_download_link')
        call_back_url = request_json.get("call_back_url")
        serving_task_code = request_json.get("serving_task_code")
        dataset_id = request_json.get("dataset_id")
        message = ""

        response = requests.get(file_url, verify=False)
        content_type = response.headers.get('Content-Type')
        dataset = ""
        
        print(response.status_code)
        state_code = response.status_code
        if state_code == 200:
            csv_data = response.text
            dataset = pl.read_csv(StringIO(csv_data))
            print("download data: ", dataset)
            if isinstance(dataset, pl.DataFrame):
                g_dataset_map[dataset_code] = (dataset,"")
                message = "get file success"
            else:
                message = "get file failed"
        elif content_type == 'application/json':
            message = response.json()
            print(message)
            logging.info(f"get file url response : {message}")
        else:
            print(response)
            message = "get file failed"
            logging.warning(f"{dataset_code} download failed, {response}")
    except Exception as e:
        state_code = 600
        message = str(e)
        print(message)
        logging.error(message, exc_info=True)
    callback_data = {
        "state_code" : state_code,
        "serving_task_code": serving_task_code,
        "dataset_id": dataset_id,
        "dataset_code": dataset_code,
        "message": message,
    }
    headers = {'Content-Type': 'application/json'}
    requests.post(call_back_url, headers=headers, json=callback_data, verify=False)
    
@app.route('/tee/file/download', methods=['POST'])
def download_dataset():
    
    dataset_code = str(uuid.uuid4())  
    print(dataset_code)
    thread = threading.Thread(target=task_download_file, kwargs={"request_json" : request.get_json(), "dataset_code" : dataset_code})
    thread.start()
    response = {  
        "data": {  
            "dataset_code": dataset_code  
        },   
        "msg" : "Dataset download ok",
        "code": 200  
    }  
    logging.info(f"dataset code: {dataset_code}")
    return jsonify(response)

def compute_fun(dataset, feature_list, alg_list):
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
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    return result
import re

def evaluate_rule(config):
    feature_ = config['feature']
    type_ = config['type']
    condition_ = config['condition']
    value_ = config['value']
    
    type_ = type_.lower()
    print("data type : ", type_)
    if type_ == 'string':
        escaped_value = r"{}".format(re.escape(value_))
        if condition_ == "包含":
            expression = pl.col(feature_).str.contains(escaped_value)
        elif condition_ == "不包含":
            expression = ~pl.col(feature_).str.contains(escaped_value)
        elif condition_ == "等于":
            expression = pl.col(feature_) == value_
        print("evaluate_rule expression", expression)
        return expression
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

    
    # 使用 eval 方法根据 condition_ 直接生成表达式
    if condition_ == '=': condition_ = '=='
    expression = f"pl.col('{feature_}') {condition_} value_"
    return eval(expression)

def task_stat(request_json):
    global g_dataset_map
    if g_dataset_map is None: 
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        
        conditional_config = request_json.get('conditional_config')
        for config in conditional_config:
            dataset_code = config['dataset_code']
            df,tmp_info = g_dataset_map[dataset_code]
            df = df.filter(evaluate_rule(config))
            g_dataset_map[dataset_code] = (df,tmp_info)
        #合并两个数据集
        merge_config = request_json.get('merge_config')
        aggregated_config = request_json.get('aggregated_config')
        result = {}
        if merge_config is not None:
            dataset_list = merge_config['dataset_list'] 
            type_ = merge_config['type']  
            feature_list = merge_config['feature_list']  
            feature_common_field = merge_config['feature_common_field']  
            
            if len(dataset_list) < 2:
                return jsonify({"status": "error", "message": "Dataset list less than 2"}), 500 
            if dataset_list[0] not in g_dataset_map and dataset_list[1] not in g_dataset_map:
                return jsonify({"status": "error", "message": "No matching dataset"}), 500 
            dataset0,_ = g_dataset_map[dataset_list[0]]
            dataset1,_ = g_dataset_map[dataset_list[1]]
            if feature_list:
                for index, (name0, name1) in enumerate(feature_list):
                    if name0 in dataset0.columns:
                        dataset0 = dataset0.rename({name0: feature_common_field[index]})
                    if name1 in dataset1.columns:
                        dataset1 = dataset1.rename({name1: feature_common_field[index]})
            merged_dataset = pl.concat([dataset0, dataset1], how='align')

            if aggregated_config:
                feature_list = aggregated_config['feature_list']
                computing_config = aggregated_config['computing_config']
                result = compute_fun(merged_dataset, feature_list, computing_config)

        reply_callback(result, request_json)
        logging.info(f"union result: {result}")
        return
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)

@app.route('/tee/stat/compute', methods=['POST'])
def handle_stat_query():
    thread = threading.Thread(target=task_stat, kwargs={"request_json" : request.get_json()})
    thread.start()
    return jsonify({"status": "success", "message": "STAT OK"}), 200
    

def task_psi(request_json):
    global g_dataset_map
    result_list = []
    if g_dataset_map is None: 
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        preprocessing_config = request_json.get('preprocessing_config')
        null_value = preprocessing_config['null_value']
        duplicate_value = preprocessing_config['duplicate_value']
        case_insensitive = preprocessing_config['case_insensitive']
        relational_config = request_json.get('relational_config')
        # condition_results[config['ruleName']] = tmp_rule
        if 'conditionConfigs' in request_json:
            for conditionConfig in request_json.get('conditionConfigs'):

                df,tmp_info = g_dataset_map[conditionConfig['dataset_code']]
                if not conditionConfig['conditionalConfig']:
                    break
                combined_condition = conditionConfig['conditionalRuleConfig']
                if combined_condition.strip():
                    condition_results = []
                    
                    for i, config in enumerate(conditionConfig['conditionalConfig']):
                        rule_name = config['ruleName']
                        tmp_rule = evaluate_rule(config)
                        condition_results.append(tmp_rule)
                        combined_condition = combined_condition.replace(rule_name, f'condition_results[{i}]')

                    combined_condition = combined_condition.replace('&&', '&').replace('||', '|')
                    print(f"Combined condition: {combined_condition}")
                    expression = eval(combined_condition)
                    print(f"expression: {expression}")
                    g_dataset_map[conditionConfig['dataset_code']] = (df.filter(expression),tmp_info)
                else:
                    print("Combined condition is empty.")
                    condition_results = []
                    for i, config in enumerate(conditionConfig['conditionalConfig']):
                        tmp_rule = evaluate_rule(config)
                        condition_results.append(tmp_rule)
                    # 如果 combined_condition 为空，用 & 连接所有的规则
                    combined_condition = '&'.join([f'condition_results[{i}]' for i in range(len(condition_results))])
                    print(f"Combined condition: {combined_condition}")
                    expression = eval(combined_condition)
                    print(f"expression: {expression}")
                    g_dataset_map[conditionConfig['dataset_code']] = (df.filter(expression),tmp_info)

        for config in relational_config:
            dataset_list = config.get('dataset_list')
            id_list = config.get('id_list')
            common_field = config.get('common_field')
            df1,tmp_info = g_dataset_map[dataset_list[0]]
            if len(dataset_list) == 0:
                return jsonify({"status": "error", "message": "the dataset_list is null"}), 500
            else:
                df1 = df1.rename({id_list[0]: common_field})
                df1 = df1.select(["id"])
                if case_insensitive and df1.schema[common_field] == pl.Utf8:
                    df1 = df1.with_columns(
                        pl.col(common_field).map_elements(
                            lambda s: s.lower() if isinstance(s, str) else s,
                            return_dtype=pl.Utf8  
                        ).alias(common_field)
                    )
                if duplicate_value:
                    df1 = df1.select(["id"]).unique(maintain_order=True)
                if null_value:
                    df1 = df1.drop_nulls(subset=["id"])
                g_dataset_map[dataset_list[0]] = (df1,tmp_info)
                if len(dataset_list) > 1:
                    df2,_ = g_dataset_map[dataset_list[1]]
                    df2 = df2.rename({id_list[1]: common_field})
                    df1_part = df1.select([pl.col(common_field)])
                    if case_insensitive and df2.schema[common_field] == pl.Utf8 :
                        df2_part = df2.select([pl.col(common_field).map_elements(lambda s: s.lower() if isinstance(s, str) else s, return_dtype=pl.Object)])
                    else:
                        df2_part = df2.select([pl.col(common_field)])

                    intersected_df = df1_part.join(df2_part, on=common_field, how="inner")

                    values = intersected_df[common_field].to_list()
                    df1 = df1.filter(pl.col(common_field).is_in(values))
                    df2 = df2.filter(pl.col(common_field).is_in(values))
                    
                    if common_field in df1.columns:
                        result_list.append(df1[common_field].to_list())

        
        result_config = request_json.get('result_config')
        if result_config:
            full_ = result_config['full']
            if 'amount' in result_config:
                amount_ = result_config['amount']
            if full_ == False:
                result_list = result_list[:amount_]
            if null_value == True:
                result_list = list(filter(None, result_list))
        reply_callback(result_list, request_json)
    except Exception as e:
        print("Exception error", str(e))
        reply_callback(result_list, request_json, str(e))
        logging.error(str(e), exc_info=True)


@app.route('/tee/psi/compute', methods=['POST'])
def handle_psi_query():
    thread = threading.Thread(target=task_psi, kwargs={"request_json" : request.get_json()})
    thread.start()
    return jsonify({"status": "success", "message": "PSI OK"}), 200

def task_pir(request_json):
    global g_dataset_map
    if g_dataset_map is None:  
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        
        dataset_code_ = request_json.get('dataset_code')
        dataset,tmp_info = g_dataset_map[dataset_code_]
        conditional_config_ = request_json.get('conditional_config')
        
        ctx = pl.SQLContext(register_globals=True, eager_execution=True)
        for config in conditional_config_:
            expression = evaluate_rule(config)
            dataset = dataset.filter(expression)
            g_dataset_map[dataset_code_] = (dataset, tmp_info)
        query_config_ = request_json.get('query_config')
        result_list = {}
        
        if query_config_:
            query_key_ = query_config_['query_key']
            query_values_ = query_config_['query_values']
            return_fields_ = query_config_['return_fields']
            ctx.register("dataset", dataset)
            
            if dataset[query_key_[0]].dtype == pl.Utf8:
                query_values_ = ', '.join([f"'{value}'" for value in query_values_])
            else:
                query_values_ = ', '.join([f"{value}" for value in query_values_])
            sql_query = f"SELECT * FROM dataset WHERE {query_key_[0]} IN ({query_values_})"
            print(sql_query)
            result = ctx.execute(sql_query)
        
            key_column  = query_key_[0]
            # value_columns = [col for col in result.columns if col != key_column]
            data_dict = {}
            for i in range(len(result)):
                key = result[key_column][i]
                values = {col: result[col][i] for col in return_fields_}
                data_dict[key] = values

            # 构建为新的列表
            result_list = [{key: values} for key, values in data_dict.items()]
            print(result_list)
        reply_callback(result_list, request_json)
        logging.info(f"pir result: {result_list}")
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)

@app.route('/tee/pir/compute', methods=['POST'])
def handle_pir_query():
    thread = threading.Thread(target=task_pir, kwargs={"request_json" : request.get_json()})
    thread.start()
    return jsonify({"status": "success", "message": "PIR OK"}), 200

@app.route('/tee/get_result/file', methods=['POST'])
def get_result_file():
    msg = ""
    output_file_path = ""
    status = ""
    try:
        global g_dataset_map
        dataset_code_ = request.json.get('dataset_code')
        # dataset = g_dataset_map[dataset_code_]
        dataset,_ = g_dataset_map.pop(dataset_code_, None)
        if dataset is None:
            print(f"No dataset found for code: {dataset_code_}")
            msg = "dataset is empty"
            raise ValueError(msg)
        else:
            print(f"Dataset for code {dataset_code_} has been retrieved and deleted.")

        file_name = "processed_" + dataset_code_ + ".csv"
        output_path = "/data/storage/dataset/pre_deal_result/"
        os.makedirs(output_path, exist_ok=True)
        output_file_path = os.path.join(output_path, file_name)
        dataset.write_csv(output_file_path)
        status = "success"
        msg = "Pre deal data successfully"
    except Exception as e:
        status = "failed"
        msg = str(e)
        print(msg)
        logging.error(msg, exc_info=True)
    return jsonify({"status": status, "message": msg, "data_result": output_file_path})

class HealthCheckFilter(logging.Filter):
    def filter(self, record):
        # 过滤掉包含 "/healthz" 的日志消息
        return "/healthz" not in record.getMessage()

# 中间件：记录请求信息
@app.before_request
def log_request_info():
    
    if request.method == "POST":
        logging.info(f"Request: {request.method} {request.url}")
        logging.info(f"Request body: {request.get_data(as_text=True)}")
        
if __name__ == '__main__':
    print(g_server_version)
    logging.basicConfig(filename='/home/app/logs/tee-sql-service/app.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logging.info(g_server_version)
    logger = logging.getLogger(__name__)

    # 添加过滤器到日志记录器
    health_check_filter = HealthCheckFilter()
    logger.addFilter(health_check_filter)
    
    mlflow.set_tracking_uri(mlflow_addr)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    experiment_id = experiment.experiment_id if experiment else None
    if not experiment_id:
        experiment_id = mlflow.create_experiment(name=experiment_name)

    app.run(debug=True, host='0.0.0.0', port=6700)
