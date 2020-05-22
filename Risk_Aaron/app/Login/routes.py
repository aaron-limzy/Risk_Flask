
from flask import render_template, flash, redirect, url_for, request, Blueprint
from werkzeug.urls import url_parse

from app.Login.login_forms import CreateUserForm, LoginForm, EditDetailsForm

from flask_login import current_user, login_user, logout_user, login_required
from app.Login.login_models import User, load_user, flask_users


from flask_sqlalchemy import SQLAlchemy

from app.decorators import roles_required

#db = SQLAlchemy()  # <--- The db object belonging to the blueprint
from app.extensions import db

login_bp = Blueprint('login', __name__)


@login_bp.route('/login', methods=['GET', 'POST'])       # Login Page. If user is login-ed, will re-direct to index.
def login():

    if current_user.is_authenticated:
        return redirect(url_for('main_app.index'))
    # else:
        # print("User is not authenticated. ")

    form = LoginForm()
    if form.validate_on_submit():
        user = load_user(username=form.username.data.lower())
        if user is None or not user.check_password(form.password.data): # If Login error
            flash('Invalid username or password')
            return redirect(url_for('login.login'))
        else:
            flash('Login successful')
            login_user(user, remember=form.remember_me.data)

            next_page = request.args.get('next')
            if not next_page or url_parse(next_page).netloc != '':
                next_page = url_for('main_app.index')
            return redirect(next_page)

    return render_template('General_Form.html', title='Sign In',header="Sign In", form=form)


@login_bp.route('/create_user', methods=['GET', 'POST'])       # Login Page. If user is login-ed, will re-direct to index.
@roles_required()
def Create_User():

    form = CreateUserForm()

    if form.validate_on_submit():

        username = form.username.data.lower()
        email = form.email.data
        password = form.password.data
        role = form.role.data
        admin_rights = 0

        # Check if the user has been created before.
        # If the Login ID is taken
        user = flask_users.query.filter_by(username=username).first()
        if user is not None:
            flash("User {} is taken. Kindly choose another username.".format(username))  # Put a message out that there is some error.
        else:
            sql_insert = "INSERT INTO  aaron.`flask_login` (`username`, `email`, `password_hash`, `admin_rights`, role) VALUES" \
                " ('{}','{}','{}','{}', '{}')".format(username, email, User.hash_password(password=password), admin_rights, role)
            print(sql_insert)
            raw_insert_result = db.engine.execute(sql_insert)
            flash("User {} has been created.".format(username))  # Put a message out that there is some error.

    return render_template('General_Form.html', title='Create User', header="Create User", form=form)


@login_bp.route('/logout', methods=['GET', 'POST'])  # Will logout the user.
@login_required
def logout():
    logout_user()
    return redirect(url_for('login.login'))




# Want to let user be able to update their own details.
@login_bp.route('/Edit_Details', methods=['GET', 'POST'])       # Login Page. If user is login-ed, will re-direct to index.
@login_required
def Edit_Details():

    form = EditDetailsForm()
    form.email.data = current_user.email
    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data

        sql_insert = "UPDATE  aaron.`flask_login` SET `email` = '{email}', `password_hash` = '{password_hash}' WHERE `username` ='{username}' ".format(
            email=email, password_hash=User.hash_password(password=password), username=current_user.username)
        print(sql_insert)
        raw_insert_result = db.engine.execute(sql_insert)
        flash("'{}' details has been Updated.".format(current_user.username))  # Put a message out that there is some error.

    return render_template('General_Form.html', title='User Edit Details', header="Edit Details", form=form)