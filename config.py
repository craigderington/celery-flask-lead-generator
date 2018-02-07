import os

BASEDIR = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    SECRET_KEY = os.urandom(32)

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:deadbeef@localhost:3306/celery_example'
    # SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', SQLITE_DB)

    CELERY_TIMEZONE = 'US/Eastern'
    CELERY_BROKER_URL = 'amqp://localhost'
    CELERY_RESULT_BACKEND = 'amqp://localhost'
    CELERY_SEND_TASK_SENT_EVENT = True


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:deadbeef@localhost:3306/celery_example'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    CELERY_TIMEZONE = 'US/Eastern'
    CELERY_BROKER_URL = 'amqp://localhost'
    CELERY_RESULT_BACKEND = 'amqp://localhost'
    CELERY_SEND_TASK_SENT_EVENT = True

    MONGODB_URI = 'localhost'
    MONGO_DB = 'earl-pixel-tracker'


class ProductionConfig(Config):
    pass


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}
