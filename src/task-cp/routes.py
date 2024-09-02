from flask import jsonify, request
import threading
import utils
from config import Config
from task_process import *
from data_process import *

def setup_routes(app):
    @app.route('/healthz')
    def health_api():
        return 'OK'

    @app.route('/version')
    def get_version():
        return jsonify({"msg": Config.SERVER_VERSION})

    @app.route('/tee/filename/load', methods=['POST'])
    def load_filename():
        return local_file_load(request)

    @app.route('/tee/file/upload', methods=['POST'])
    def load_dataset():
        return load_dataset_handler(request)

    @app.route('/tee/file/download', methods=['POST'])
    def download_dataset():
        dataset_code = utils.hash_string(request.json.get('dataset_download_link'))
        thread = threading.Thread(target=task_download_file, kwargs={"request_json" : request.get_json(), "dataset_code" : dataset_code})
        thread.start()
        response = {  
            "data": {"dataset_code": dataset_code},   
            "msg" : "Dataset download ok",
            "code": 200  
        }  
        return jsonify(response)
    
    @app.route('/tee/stat/compute', methods=['POST'])
    def handle_stat_query():
        thread = threading.Thread(target=task_stat, kwargs={"request_json" : request.get_json()})
        thread.start()
        return jsonify({"status": "success", "message": "STAT OK"}), 200

    @app.route('/tee/psi/compute', methods=['POST'])
    def handle_psi_query():
        thread = threading.Thread(target=task_psi, kwargs={"request_json" : request.get_json()})
        thread.start()
        return jsonify({"status": "success", "message": "PSI OK"}), 200

    @app.route('/tee/pir/compute', methods=['POST'])
    def handle_pir_query():
        thread = threading.Thread(target=task_pir, kwargs={"request_json" : request.get_json()})
        thread.start()
        return jsonify({"status": "success", "message": "PIR OK"}), 200
    
    @app.route('/tee/pir/sync-compute', methods=['POST'])
    def handle_sync_pir_query():
        response = task_pir(request.get_json(), False)
        return jsonify(response), 200
    
    @app.route('/tee/get_result/file', methods=['POST'])
    def get_result_file():
        return get_result_file_handler(request)
