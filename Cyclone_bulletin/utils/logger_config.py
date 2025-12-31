import logging
import os


def setup_logger(name: str = "cyclone_pipeline") -> logging.Logger:
    """
    Sets up and returns a logger that logs to:
    - logs/errorlog.log
    - console (stdout)
    """

    # Create logs directory in present directory
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "errorlog.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs if logger is reused
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
