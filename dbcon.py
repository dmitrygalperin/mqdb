from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DbConfig
from resources import User
import sys
import logging

logging.basicConfig(filename='orm.log',level=logging.INFO, format='%(asctime)s %(message)s')

USERNAME = DbConfig.username
PASSWORD = DbConfig.password
HOST     = DbConfig.host
DB_TYPE  = DbConfig.db_type
DB_NAME  = DbConfig.db_name

#Register SQLAlchemy resources here
RESOURCES = {
    'user': User
}


class Dbcon(object):

    '''Static class that creates SQLAlchemy engine and session'''

    logger = logging.getLogger('dbcon')
    logger.addHandler(logging.StreamHandler())

    @classmethod
    def get_engine(cls):
        try:
            engine = create_engine(f'{DB_TYPE}://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}')
            engine.connect()
            return engine
        except: #TODO: Catch specific errors
            logger.critical(sys.exc_info())
            return

    @classmethod
    def get_session(cls):
        engine = cls.get_engine()
        if engine:
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            return

    @classmethod
    def get_resource(cls, resource_name):
        return RESOURCES.get(resource_name)
