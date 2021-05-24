import os
from Aaron_Lib import *
from datetime import timedelta

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or "DSF#23kjsdh8aSAd@34asdf8akl23j48sdla"
    #SECRET_KEY = os.environ.get('SECRET_KEY') or "lalala-We-don't-know-what-it-isss"

    if get_machine_ip_address() == '192.168.64.73':
        SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://Aaron_Local:aaron_local@localhost/aaron'

        SQLALCHEMY_BINDS = {
            'test': 'mysql+pymysql://Aaron_Local:aaron_local@localhost/test',
            'live1': 'mysql+pymysql://Aaron_Local:aaron_local@localhost/live1',
            'live2': 'mysql+pymysql://Aaron_Local:aaron_local@localhost/live2',
            'live3': 'mysql+pymysql://Aaron_Local:aaron_local@localhost/live3',
            'live5': 'mysql+pymysql://Aaron_Local:aaron_local@localhost/live5',
            'mt5_live1': 'mysql+pymysql://risk:1qaz2wsx@119.81.149.213/mt5'
        }

        print("On server (64.73). Will use local host connection.")
    else:
        SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/aaron'

        SQLALCHEMY_BINDS = {
            'test': 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/test',
            'live1': 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/live1',
            'live2': 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/live2',
            'live3': 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/live3',
            'live5': 'mysql+pymysql://mt4:1qaz2wsx@192.168.64.73/live5',
            'mt5_live1' : 'mysql+pymysql://risk:1qaz2wsx@119.81.149.213/mt5'
        }
        print("Not on Server (64.73) Will use Internal IP to link SQL. ")



    # Don't need to signal the app whenever there is a change.
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_POOL_RECYCLE = 300
    SQLALCHEMY_ENGINE_OPTIONS = {
        "max_overflow": 100,
        "pool_pre_ping": True,
        "pool_recycle": 60 * 60,
        "pool_size": 30,
        #"pool_timeout" : 43200,
    }


    UPLOAD_FOLDER = '/path/to/the/uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    UPLOADS_DEFAULT_DEST='./project/static/Files/'
    UPLOADS_DEFAULT_URL= 'http://localhost:5000/static/Files/'

    UPLOADED_SWAPS_DEST='./project/static/vantage_swap/'
    UPLOADED_SWAPS_URL='http://localhost:5000/static/vantage_swap/'
    VANTAGE_UPLOAD_FOLDER="./Swaps_upload/Vantage_upload/"

    MAIL_SERVER = "smtp.googlemail.com"
    MAIL_PORT = 587
    MAIL_USE_TLS = 1
    MAIL_USERNAME = "aaron.riskbgi@gmail.com"
    MAIL_PASSWORD = "ReportReport"
    ADMINS = ['aaron.lim@blackwellglobal.com']

    # Set so that each time flask is ended and ran (in cmd)
    # Can be used to refresh the cookies.
    FLASK_UPDATE_TIMING = "{}".format(datetime.datetime.now())

    # # To send email when there are server issues.
    # MAIL_SERVER = os.environ.get('MAIL_SERVER')
    # MAIL_PORT = int(os.environ.get('MAIL_PORT') or 25)
    # MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
    # MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    # MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    # ADMINS = ['aaron.lim@blackwellglobal.com']
    CFH_BO_PASSWORD = "Bgil8888!!"

    # If set to True the cookie is refreshed on every request, which bumps the lifetime.
    # Works like Flask’s SESSION_REFRESH_EACH_REQUEST.
    REMEMBER_COOKIE_REFRESH_EACH_REQUEST=True

    SESSION_COOKIE_SAMESITE = None

    # WE don't need to set this as Flask internally sets this to 31 days.
    #PERMANENT_SESSION_LIFETIME = timedelta(minutes=1)

    #WTF_CSRF_TIME_LIMIT= None






