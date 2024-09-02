import logging
import threading

# 创建一个线程本地存储对象
thread_local = threading.local()

class TaskCodeAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # 从线程本地存储中获取 task_code
        task_code = getattr(thread_local, 'task_code', 'DEFAULT_TASK_CODE')
        return f"[{task_code}] {msg}", kwargs

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除现有的处理器，防止重复添加
    if logger.hasHandlers():
        logger.handlers.clear()
        
    file_handler = logging.FileHandler('/home/app/logs/tee-sql-service/app.log')
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # logger.handlers.clear()
    return logger

def get_logger_with_task_code():
    # 获取线程本地存储中的 task_code
    task_code = getattr(thread_local, 'task_code', 'DEFAULT_TASK_CODE')
    logger = logging.getLogger(__name__)
    return TaskCodeAdapter(logger, {'task_code': task_code})

def set_task_code(task_code):
    # 设置线程本地存储中的 task_code
    thread_local.task_code = task_code