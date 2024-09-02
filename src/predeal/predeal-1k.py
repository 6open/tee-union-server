from flask import Flask, jsonify, request
import polars as pl
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData  
import uuid, json
import threading  
from polars import DataFrame  
import sys
import logging
import time
import ujson
import requests
import os
import boto3
import io
from io import BytesIO  
from io import StringIO  

app = Flask(__name__)
server_version = "PREDEAL_1K: 1.0.5"

g_dataset_map = {}

@app.route('/healthz')
def health_api():
    return 'OK'

@app.route('/version')
def get_version():
    print(server_version)
    response = {"msg" : server_version}
    return jsonify(response)

@app.route('/tee/filename/load', methods=['POST'])
def load_filename():
    global g_dataset_map  
    try:
        dataset_code = str(uuid.uuid4())
        dataset = None
        data_info = request.json.get('filename')
        if 's3Info' not in data_info:
            file_name = data_info
            file_name = os.path.join("/data/storage/dataset", file_name)
            dataset = pl.read_csv(file_name)
            print(dataset)
        else:
            s3_data = json.loads(data_info)
            print(s3_data)
            s3_info = s3_data['s3Info']
            s3 = boto3.client(
                's3',
                endpoint_url=s3_info["endpoint"],
                aws_access_key_id=s3_info["accessKey"],
                aws_secret_access_key=s3_info["secretKey"]
            )
            print(s3_info)
            file_name = s3_info["fileName"]
            obj = s3.get_object(Bucket=s3_info["bucketName"], Key=file_name)
            dataset = pl.read_csv(BytesIO(obj['Body'].read()))
        lock = threading.Lock()  
        with lock:
            print(dataset)
            ctx = pl.SQLContext(register_globals=True, eager_execution=True)
            g_dataset_map[dataset_code] = (ctx, dataset, data_info)
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
    print(dataset)
    global g_dataset_map  
    try:
        dataset_code = str(uuid.uuid4())  
        ctx = pl.SQLContext(register_globals=True, eager_execution=True)
        lock = threading.Lock()  
        with lock:  
                g_dataset_map[dataset_code] = (ctx, dataset, None)
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

@app.route('/tee/file/download', methods=['POST'])
def download_dataset():
    file_url = request.json.get('dataset_download_link')
    call_back_url = request.json.get("call_back_url")
    serving_task_code = request.json.get("serving_task_code")
    dataset_id = request.json.get("dataset_id")
    
    response = requests.get(file_url, verify=False)
    
    csv_data = response.text
    dataset = pl.read_csv(StringIO(csv_data))
    
    global g_dataset_map  
    try:
        dataset_code = str(uuid.uuid4())  
        # dataset_code = file_url
        ctx = pl.SQLContext(register_globals=True, eager_execution=True)
        lock = threading.Lock()  
        with lock:  
                g_dataset_map[dataset_code] = (ctx, dataset, None)
        headers = {'Content-Type': 'application/json'}
        callback_data = {
            "serving_task_code": serving_task_code,
            "dataset_id": dataset_id,
            "dataset_code": dataset_code
        }
        callback_result = requests.post(call_back_url, headers=headers, json=callback_data)
        if callback_result.status_code == 200:
            msg = "Dataset download ok"
        else:
            msg = "Callback failed"
        
        response = {  
            "data": {  
                "dataset_code": dataset_code  
            },  
            "msg": msg,  
            "code": 200  
        }  
        logging.info(f"dataset code: {dataset_code}")
        return jsonify(response)
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


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
    
@app.route('/tee/sql/compute', methods=['POST'])
def handle_sql_query():
    global g_dataset_map
    if g_dataset_map is None:  # 添加判断语句，检查dataset是否为空  
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        #筛选数据集
        
        conditional_config = request.json.get('conditional_config')
        for config in conditional_config:
            dataset_code = config['dataset_code']
            feature_ = config['feature']
            type_ = config['type']
            condition_ = config['condition']
            value_ = config['value']
            print(g_dataset_map)
            ctx,dataset,data_info = g_dataset_map[dataset_code]
            if type_ == 'str':
                if condition_ == "包含":
                    dataset = dataset.filter(dataset[feature_].str.contains(value_))
                elif condition_ == "不包含":
                    dataset = dataset.filter(~dataset[feature_].str.contains(value_))
                elif condition_ == "等于":
                    dataset = dataset.filter(dataset[feature_] == value_)
            else :
                sql_query = "SELECT * FROM {} WHERE {} {} {}".format("dataset", feature_, condition_, value_)
                print(sql_query)
                ctx.register("dataset", dataset)
                dataset = ctx.execute(sql_query)
            g_dataset_map[dataset_code] = (ctx, dataset,data_info)
            # print(dataset)
        #合并两个数据集
        merge_config = request.json.get('merge_config')
        aggregated_config = request.json.get('aggregated_config')
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
            ctx0,dataset0,_ = g_dataset_map[dataset_list[0]]
            ctx1,dataset1,_ = g_dataset_map[dataset_list[1]]
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
        logging.info(f"union result: {result}")
        return jsonify({"status": "success", "message": "Processing stat query successfully", "statistic_result": result}) 
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

    
@app.route('/tee/psi/compute', methods=['POST'])
def handle_psi_query():
    global g_dataset_map
    if g_dataset_map is None:  # 添加判断语句，检查dataset是否为空  
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        result_list = []
        preprocessing_config = request.json.get('preprocessing_config')
        null_value = preprocessing_config['null_value']
        duplicate_value = preprocessing_config['duplicate_value']
        case_insensitive = preprocessing_config['case_insensitive']
        
        relational_config = request.json.get('relational_config')
        df1 = pl.DataFrame()
        df2 = pl.DataFrame()
        tmp_result = pl.DataFrame()
        merged_column = "merged_column"
        for config in relational_config:
            dataset_list = config.get('dataset_list')
            id_list = config.get('id_list')
            common_field = config.get('common_field')
            if len(dataset_list) == 1:
                ctx,df1,info = g_dataset_map[dataset_list[0]]
                df1 = df1.rename({id_list[0]: common_field})
                g_dataset_map[dataset_list[0]] = (ctx, df1, info)
                break

            _,df1,_ = g_dataset_map[dataset_list[0]]
            _,df2,_ = g_dataset_map[dataset_list[1]]
            
            df1 = df1.rename({id_list[0]: common_field})
            df2 = df2.rename({id_list[1]: common_field})
            
            if tmp_result.is_empty(): tmp_result = df1
            start_time = time.time()
            if "merged_column" in df1.columns:
                merged_column = df1["merged_column"].cast(str) + "0" + df1[common_field].cast(str)
                df1 = df1.with_columns(merged_column.cast(int).alias("merged_column"))
                merged_column = df2["merged_column"].cast(str) + "0" + df2[common_field].cast(str)
                df2 = df2.with_columns(merged_column.cast(int).alias("merged_column"))
            else:
                df1 = df1.with_columns(df1[common_field].alias("merged_column"))
                df2 = df2.with_columns(df2[common_field].alias("merged_column"))
                
            if case_insensitive:
                df1_part = df1.select([pl.col("merged_column").map_elements(lambda s: s.lower() if isinstance(s, str) else s)])
                df2_part = df2.select([pl.col("merged_column").map_elements(lambda s: s.lower() if isinstance(s, str) else s)])
            else:
                df1_part = df1.select([pl.col("merged_column")])
                df2_part = df2.select([pl.col("merged_column")])

            start_time = time.time()
            intersected_df = df1_part.join(df2_part, on="merged_column", how="inner")
            values = intersected_df["merged_column"].to_list()
            df1 = df1.filter(pl.col("merged_column").is_in(values))
            df2 = df2.filter(pl.col("merged_column").is_in(values))
            if not values:
                break
            result_count = len(result_list)
        for config in relational_config:
            common_field = config["common_field"]
            if common_field in df1.columns:
                result_list.append(df1[common_field].to_list())
        # start_time = time.time()
        result_config = request.json.get('result_config')
        if result_config:
            full_ = result_config['full']
            amount_ = result_config['amount']
            if full_ == False:
                result_list = result_list[:amount_]
            if null_value == True:
                # result_list = list(set(result_list))
                result_list_without_empty = list(filter(None, result_list))
        result = ujson.dumps(result_list)
        logging.info(f"psi result: {result}")
        # print(f"time(ms)-格式转换: {(time.time() - start_time) * 1000:.2f}")
        # print("result_count : ", result_count)
        return jsonify({"status": "success", "message": "Processing psi query successfully", "psi_result": result})
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    
    
@app.route('/tee/pir/compute', methods=['POST'])
def handle_pir_query():
    global g_dataset_map
    if g_dataset_map is None:  # 添加判断语句，检查dataset是否为空  
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        dataset_code_ = request.json.get('dataset_code')
        ctx,dataset,data_info = g_dataset_map[dataset_code_]
        
        conditional_config_ = request.json.get('conditional_config')
        final_result = None
        for config_ele in conditional_config_:
            feature_ = config_ele['feature']
            type_ = config_ele['type']
            condition_ = config_ele['condition']
            value_ = config_ele['value']
            
            if type_ == 'str':
                if condition_ == "包含":
                    dataset = dataset.filter(dataset[feature_].str.contains(value_))
                elif condition_ == "不包含":
                    dataset = dataset.filter(~dataset[feature_].str.contains(value_))
                elif condition_ == "等于":
                    dataset = dataset.filter(dataset[feature_] == value_)
            else :
                # Check if value is numeric or string and format accordingly
                if condition_ in ['=', '!=', '<', '>', '<=', '>='] and isinstance(value_, str):
                    try:
                        float(value_)  # Check if value_ can be converted to float
                    except ValueError:
                        value_ = f"'{value_}'"  # If not, it's a string and should be quoted
                else:
                    value_ = f"'{value_}'"

                sql_query = f"SELECT * FROM dataset WHERE {feature_} {condition_} {value_}"
                print(sql_query)
                dataset = ctx.execute(sql_query)
                print(dataset)

            ctx.register("dataset", dataset)
            g_dataset_map[dataset_code_] = (ctx, dataset, data_info)
        
        query_config_ = request.json.get('query_config')
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
            # print("运行到行号：",sys._getframe().f_lineno)
        
            key_column  = query_key_[0]
            value_columns = [col for col in result.columns if col != key_column]
            data_dict = {}
            for i in range(len(result)):
                key = result[key_column][i]
                values = {col: result[col][i] for col in value_columns}
                data_dict[key] = values

            # 构建为新的列表
            result_list = [{key: values} for key, values in data_dict.items()]
        print(result_list)
        logging.info(f"pir result: {result_list}")
        return jsonify({"status": "success", "message": "Processing pir query successfully", "pir_result": result_list})
    except Exception as e:
        print(str(e))
        logging.error(str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/tee/get_result/file', methods=['POST'])
def get_result_file():
    global g_dataset_map
    dataset_code_ = request.json.get('dataset_code')
    print(g_dataset_map[dataset_code_])
    _, dataset,data_info = g_dataset_map[dataset_code_]
    file_name = "processed_" + dataset_code_ + ".csv"
    if 's3Info' not in data_info:
        output_path = "/data/storage/dataset/pre_deal_result/"
        os.makedirs(output_path, exist_ok=True)
        output_file_info = os.path.join(output_path, file_name)
        dataset.write_csv(output_file_info)
    else :
        s3_data = json.loads(data_info)
        print(s3_data)
        s3_info = s3_data['s3Info']
        s3 = boto3.client(
            's3',
            endpoint_url=s3_info["endpoint"],
            aws_access_key_id=s3_info["accessKey"],
            aws_secret_access_key=s3_info["secretKey"]
        )
        csv_buffer = io.BytesIO()
        dataset.write_csv(csv_buffer)
        csv_buffer.seek(0)  # 重置缓冲区指针到开始位置
        s3.put_object(Bucket=s3_info["bucketName"], Key=file_name, Body=csv_buffer.getvalue())
        print("DataFrame 已成功上传到 S3!")
        s3_info["fileName"] = file_name
        s3_data['s3Info'] = s3_info
        output_file_info = json.dumps(s3_data)
    return jsonify({"status": "success", "message": "Pre deal data successfully", "data_result": output_file_info})


# 中间件：记录请求信息
@app.before_request
def log_request_info():
    logging.info(f"Request: {request.method} {request.url}")
    if request.method == "POST":
        logging.info(f"Request body: {request.get_data(as_text=True)}")
    else:
        logging.info(f"Request args: {request.args}")
        
if __name__ == '__main__':
    logging.basicConfig(filename='/home/app/logs/tee-sql-service/app.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    print(server_version)
    logging.info(server_version)
    app.run(debug=True, host='0.0.0.0', port=6700)
    # app.run(debug=False, host='0.0.0.0', port=6710)

