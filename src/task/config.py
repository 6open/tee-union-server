import os

g_dataset_map = {}

class Config:
    SERVER_FLAG = "SERVER_BP"
    VERSION_NUMBER = "1.0.15"
    SERVER_VERSION = SERVER_FLAG + ": " + VERSION_NUMBER
    MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI')
    EXPERIMENT_NAME = 'tee-sql-service'
    RESULT_NUM_LIMIT = 20
    FILE_PATH = '/data/storage/dataset/tee-psi-pir-file/'           #加载的原始文件保存到本地
    PRE_DEAL_PATH = '/data/storage/dataset/pre_deal_result/'        #预处理结果文件
    # TASK_RESULT_PATH = '/data/storage/dataset/tee-sql-task_result/' #任务处理结果文件
    TASK_RESULT_PATH = 'tee-sql-task_result/' #任务处理结果文件
    EXPERIMENT_ID = ""
