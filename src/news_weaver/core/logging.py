import logging
import json

class StructuredJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {"level": record.levelname,
                       "message": record.getMessage(),
                       "time": self.formatTime(record, self.datefmt),
                       "name": record.name}
        return json.dumps(log_record)

# Configure logging
logger = logging.getLogger('news_weaver_logger')
logger.setLevel(logging.DEBUG) # Set level to DEBUG or any desired level

console_handler = logging.StreamHandler()
console_handler.setFormatter(StructuredJsonFormatter())

logger.addHandler(console_handler)

# Example usage
if __name__ == '__main__':
    logger.info('This is an info message.')
    logger.error('This is an error message.')