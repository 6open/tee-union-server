import os

g_dataset_map = {}

class Config:
    SERVER_FLAG = "SERVER_BP"
    VERSION_NUMBER = "1.0.12.1"
    SERVER_VERSION = SERVER_FLAG + ": " + VERSION_NUMBER
    MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI')
    EXPERIMENT_NAME = 'tee-sql-service'
    RESULT_NUM_LIMIT = 20
    FILE_PATH = '/data/storage/dataset/tee-psi-pir-file/'
    PRE_DEAL_PATH = '/data/storage/dataset/pre_deal_result/'
    EXPERIMENT_ID = ""
