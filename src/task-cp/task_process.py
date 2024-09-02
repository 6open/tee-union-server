import uuid
import os
import polars as pl
from io import StringIO, BytesIO
import threading
import logging
from flask import jsonify
from utils import *
from config import *
from log import *


def get_dataset(dataset_code):
    global g_dataset_map
    if dataset_code in g_dataset_map:
        return g_dataset_map[dataset_code]
    else: 
        print(Config.FILE_PATH)
        print(dataset_code)
        file_path = Config.FILE_PATH + dataset_code
        g_dataset_map[dataset_code] = pl.read_csv(file_path)
        print(g_dataset_map[dataset_code])
        return g_dataset_map[dataset_code]
        

def task_stat(request_json):
    global g_dataset_map
    if g_dataset_map is None: 
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        task_code = request_json.get('task_code')
        set_task_code(task_code)
        logger = get_logger_with_task_code()
        
        conditional_config = request_json.get('conditional_config')
        for config in conditional_config:
            dataset_code = config['dataset_code']
            g_dataset_map[dataset_code] = g_dataset_map[dataset_code].filter(evaluate_rule(config))
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
            dataset0 = g_dataset_map[dataset_list[0]]
            dataset1 = g_dataset_map[dataset_list[1]]
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

        reply_callback(result, request_json, "")
        logger.info(f"union result: {result}")
        return
    except Exception as e:
        print(str(e))
        logger.error(str(e), exc_info=True)
        
def task_psi(request_json):
    global g_dataset_map
    tmp_dataset_map = {}
    result_list = []
    if g_dataset_map is None: 
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        task_code = request_json.get('task_code')
        set_task_code(task_code)
        logger = get_logger_with_task_code()

        preprocessing_config = request_json.get('preprocessing_config')
        null_value = preprocessing_config['null_value']
        duplicate_value = preprocessing_config['duplicate_value']
        case_insensitive = preprocessing_config['case_insensitive']
        relational_config = request_json.get('relational_config')
        
        if 'conditionConfigs' in request_json:
            for conditionConfig in request_json.get('conditionConfigs'):

                df = get_dataset(conditionConfig['dataset_code'])
                
                # df = g_dataset_map[conditionConfig['dataset_code']]
                combined_condition = conditionConfig['conditionalRuleConfig']
                
                if combined_condition is not None and combined_condition.strip():
                    print("Combined condition is empty.")
                    condition_results = []
                    if not conditionConfig['conditionalConfig']:
                        break
                    for config in conditionConfig['conditionalConfig']:
                        tmp_rule = evaluate_rule(config)
                        condition_results.append(tmp_rule)
                    for i in range(len(condition_results)):
                        combined_condition = combined_condition.replace(f'rule{i+1}', f'condition_results[{i}]')
                    # Replace logical operators
                    
                    combined_condition = combined_condition.replace('&&', '&').replace('||', '|')
                    print(f"Combined condition: {combined_condition}")
                    expression = eval(combined_condition)
                    print(f"expression: {expression}")
                    # g_dataset_map[conditionConfig['dataset_code']] = df.filter(expression)
                    tmp_dataset_map[conditionConfig['dataset_code']] = df.filter(expression)
                else:
                    print("Combined condition is empty.")
                    condition_results = []
                    if not conditionConfig['conditionalConfig']:
                        # 如果没有配置条件，跳出
                        break
                    for config in conditionConfig['conditionalConfig']:
                        tmp_rule = evaluate_rule(config)
                        condition_results.append(tmp_rule)
                    # 如果 combined_condition 为空，用 & 连接所有的规则
                    combined_condition = '&'.join([f'condition_results[{i}]' for i in range(len(condition_results))])
                    print(f"Combined condition: {combined_condition}")
                    expression = eval(combined_condition)
                    print(f"expression: {expression}")
                    # g_dataset_map[conditionConfig['dataset_code']] = df.filter(expression)
                    tmp_dataset_map[conditionConfig['dataset_code']] = df.filter(expression)

        
        for config in relational_config:
            dataset_list = config.get('dataset_list')
            id_list = config.get('id_list')
            common_field = config.get('common_field')
            intersected_df = []
            if len(dataset_list) == 0:
                return jsonify({"status": "error", "message": "the dataset_list is null"}), 500
            else:
                for index, dataset_code in enumerate(dataset_list):
                    df = []
                    if dataset_code in tmp_dataset_map:
                        df = tmp_dataset_map[dataset_code]
                    else:
                        df = get_dataset(dataset_code)
                    # df = g_dataset_map[dataset]
                    df = df.rename({id_list[index]: common_field})
                    df_part = df.select([pl.col(common_field)])
                    if case_insensitive and df_part.schema[common_field] == pl.Utf8:
                        df_part = df_part.with_columns(
                            pl.col(common_field).map_elements(
                                lambda s: s.lower() if isinstance(s, str) else s,
                                return_dtype=pl.Utf8  
                            ).alias(common_field)
                        )
                    if duplicate_value:
                        df_part = df_part.select([pl.col(common_field)]).unique(maintain_order=True)
                    if null_value:
                        df_part = df_part.drop_nulls(subset=[common_field])
                    if index == 0 :
                        intersected_df = df_part
                    else:
                        intersected_df = df_part.join(intersected_df, on=common_field, how="inner")
                        if index == len(dataset_list) - 1:
                            values = intersected_df[common_field].to_list()
                            result_list.append(values)
                        
        result_config = request_json.get('result_config')
        if result_config:
            full_ = result_config['full']
            if 'amount' in result_config:
                amount_ = result_config['amount']
            if full_ == False:
                result_list = result_list[:amount_]
            if null_value == True:
                result_list = list(filter(None, result_list))
        reply_callback(result_list, request_json, "")
    except Exception as e:
        print("Exception error", str(e))
        reply_callback(result_list, request_json, str(e))
        logger.error(str(e), exc_info=True)
        
def task_pir(request_json, callback = True):
    global g_dataset_map
    result_list = {}
    if g_dataset_map is None:  
        return jsonify({"status": "error", "message": "Dataset is not initialized"}), 500 
    try:
        task_code = request_json.get('task_code')
        set_task_code(task_code)
        logger = get_logger_with_task_code()
        
        dataset_code_ = request_json.get('dataset_code')
        # dataset = g_dataset_map[dataset_code_]
        dataset = get_dataset(dataset_code_)
        conditional_config_ = request_json.get('conditional_config')
        
        for config in conditional_config_:
            # g_dataset_map[dataset_code_] = g_dataset_map[dataset_code_].filter(evaluate_rule(config))
            dataset = dataset.filter(evaluate_rule(config))
        query_config_ = request_json.get('query_config')
        
        
        if query_config_:
            query_key_ = query_config_['query_key']
            query_values_list = query_config_['query_values']
            return_fields_ = query_config_['return_fields']
            query_values_list = query_values_list.split(',')
            column_dtype = dataset.schema[query_key_[0]]
            if column_dtype == pl.Int64:
                query_values_list = [int(val) for val in query_values_list]
            elif column_dtype == pl.Utf8:
                query_values_list = [str(val) for val in query_values_list]
            else:
                raise ValueError(f"Unsupported column data type: {column_dtype}")
            filtered_dataset = dataset.filter(pl.col(query_key_[0]).is_in(query_values_list))
            if query_key_[0] not in return_fields_:
                return_fields_.append(query_key_[0])
            result = filtered_dataset.select(return_fields_)
            
            key_column  = query_key_[0]
            data_dict = {}
            for i in range(len(result)):
                key = result[key_column][i]
                values = {col: result[col][i] for col in return_fields_}
                data_dict[key] = values

            # 构建为新的列表
            result_list = [{key: values} for key, values in data_dict.items()]
            print(result_list)
        logger.info(f"pir result: {result_list}")
        return reply_callback(result_list, request_json, "", callback)
        
    except Exception as e:
        print("Exception error", str(e))
        reply_callback(result_list, request_json, str(e))
        logger.error(str(e), exc_info=True)
