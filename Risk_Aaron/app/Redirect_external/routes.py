from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect, current_app
from flask_login import current_user, login_user, logout_user, login_required
from app.decorators import roles_required

from Helper_Flask_Lib import *

import hashlib

#pip install pycryptodome==3.4.3
from Crypto.Util.Padding import pad
from Crypto.Cipher import AES
import  base64
from Crypto import Random

re_direct = Blueprint('re_direct', __name__)



@re_direct.before_request
def before_request():

    # Don't want to record any ajax calls.
    endpoint = "{}".format(request.endpoint)
    if endpoint.lower().find("ajax") >=0:
        return
    else:

        # check if the user is logged.
        if not current_user.is_authenticated:
            return
        raw_sql = "INSERT INTO aaron.Aaron_Page_History (login, IP, full_path, datetime) VALUES ('{login}', '{IP}', '{full_path}', now()) ON DUPLICATE KEY UPDATE datetime=now()"
        sql_statement = raw_sql.format(login=current_user.id,
                                       IP=request.remote_addr,
                                       full_path=request.full_path)

        async_sql_insert_raw(app=current_app._get_current_object(),
                             sql_insert=sql_statement)


# # # To Query for all open trades by a particular symbol
# # # Shows the closed trades for the day as well.
#@re_direct.route('/testing', methods=['GET', 'POST'])
@re_direct.route('/yudi/redirect', methods=['GET', 'POST'])
@roles_required()
def yudi_test():

    url = request.args.get('url')

    if url == None: # There is no url parameter.
        flash("Redirect URL Parameter missing.")
        return redirect(url_for("login.login", _external=True))    # Redirect to login page.

    message=cipher_text()   # Get the ciptertext and randomiv
    # Want to put these into a string for Yudi's URL

    url_params = "&".join(["{k}={d}".format(k=k, d=d) for k, d in message.items()])

    #print(url)
    # Redirect to the page, but this time, add in the cipher.
    return redirect("{url}?{url_params}".format(url=url, url_params=url_params), code=307)




def cipher_text():
    # received redirection from php page with its url destination $_GET['url']

    # check token if the user is login if not asked for user validation

    # once validated pass create and encryption msg
    # -----------------------------------------SECRET SHARED VARIABLE-----------------------------
    key = "01c527b323c7f5393a1e61d74bbd781e83b48bc94836c58544e1560cf85f831b"
    key = hashlib.sha256((key).encode()).digest()  # ensure 32 bytes KEY
    word = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #print("date and time =", word)
    # -----------------------------------------------------------------------------------------
    iv = Random.new().read(AES.block_size)
    # print("key :",key.hex(), len(key))#32 bytes
    #print("iv : ", iv.hex(), len(iv))

    cipher_text = AES.new(key, AES.MODE_CBC, iv).encrypt(pad(word.encode(), 16))
    #print("encoded: ", base64.b64encode(cipher_text).decode('utf-8'), "size: ", len(cipher_text))

    # send over to the $_GET['url']
    # $_POST['ciphertext']=base64.b64encode(cipher_text).decode('utf-8')
    # $_POST['randomiv']=iv.hex()
    return {'ciphertext':base64.b64encode(cipher_text).decode('utf-8'), 'randomiv':iv.hex()}