import logging.config
import logging

from config import settings

logging.config.dictConfig(settings["logging_config"])

logger = logging.getLogger()

