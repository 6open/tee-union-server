import os
import logging
import mlflow
from flask import Flask, request, g
from config import Config
from routes import setup_routes
from log import *


app = Flask(__name__)
app.config.from_object(Config)

def setup_mlflow():
    Config.MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI')
    Config.EXPERIMENT_NAME = 'tee-sql-service'
    os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

    mlflow.set_tracking_uri(Config.MLFLOW_TRACKING_URI)
    experiment = mlflow.get_experiment_by_name(Config.EXPERIMENT_NAME)
    Config.EXPERIMENT_ID = experiment.experiment_id if experiment else None
    if not Config.EXPERIMENT_ID:
        Config.EXPERIMENT_ID = mlflow.create_experiment(name=Config.EXPERIMENT_NAME)
    
class HealthCheckFilter(logging.Filter):
    def filter(self, record):
        return "/healthz" not in record.getMessage()

@app.before_request
def log_request_info():
    if request.method == "POST":
        task_code = request.headers.get('X-Task-Code', 'DEFAULT_TASK_CODE')
        set_task_code(task_code)
        logger_adapter = get_logger_with_task_code()
        logger_adapter.info(f"Request: {request.method} {request.url}")
        logger_adapter.info(f"Request body: {request.get_data(as_text=True)}")

if __name__ == '__main__':
    print("SERVER VERSION :", Config.SERVER_VERSION)

    setup_logging()
    logger = get_logger_with_task_code()
    logger.info(Config.SERVER_VERSION)
    
    setup_mlflow()
    setup_routes(app)
    
    if not os.path.exists(Config.FILE_PATH):
        os.makedirs(Config.FILE_PATH)
    app.run(debug=True, host='0.0.0.0', port=6700)
