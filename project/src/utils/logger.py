import logging
import sys

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    # Only add handlers if they don't exist to avoid duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format: Time - Name - Level - Message
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Output to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Output to file (Requirement 7: Bad-record logging)
        file_handler = logging.FileHandler("pipeline.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger