from flask_login import LoginManager
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
import flask_excel as excel
from flask_uploads import UploadSet
from flask_bootstrap import Bootstrap

from flask_moment import Moment
#pip install simplekv
# import redis
# from simplekv.memory.redisstore import RedisStore

# Want to set SQL to read without locking.
db = SQLAlchemy()
login = LoginManager()
bootstrap = Bootstrap()
# store = RedisStore(redis.StrictRedis())
moment = Moment()
excel_format = UploadSet('files', extensions=('xls', 'xlsx', 'csv'))
#db = SQLAlchemy(server)