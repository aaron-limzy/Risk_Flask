from threading import Thread

from flask_login import current_user
from app.extensions import login
from functools import wraps
from flask import flash


def async_fun(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()
    return wrapper


# Various different rights. With Admin and Risk being the highest.
# The rest are for other teams, if they need access.
def  roles_required(roles=["Risk", "Admin", "Risk_TW"]):
    def wrapper(fn):
        @wraps(fn)
        def decorated_view(*args, **kwargs):
            if not current_user.is_authenticated:
              return login.unauthorized()
            if role_authentication(roles, current_user.role):
                flash("Kindly Contact Risk Team for access rights.")
                return login.unauthorized()
            return fn(*args, **kwargs)
        return decorated_view
    return wrapper

# So that we might be able to use this on the templates as well.
def role_authentication(roles, user_role):
    return user_role not in roles