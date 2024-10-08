#logging_config.py
import logging
import os

def setup_logging():
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_directory, "app.log")),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)

logger = setup_logging()