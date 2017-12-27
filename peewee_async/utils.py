import logging

logger = logging.getLogger('peewee.async')
logger.addHandler(logging.NullHandler())
