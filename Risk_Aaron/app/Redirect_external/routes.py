from flask import Blueprint, render_template, Markup, url_for, request, session, current_app, flash, redirect

from app.decorators import roles_required

from Helper_Flask_Lib import *

import hashlib

#pip install pycryptodome==3.4.3
from Crypto.Util.Padding import pad
from Crypto.Cipher import AES
import  base64
from Crypto import Random

re_direct = Blueprint('re_direct', __name__)


# # # To Query for all open trades by a particular symbol
# # # Shows the closed trades for the day as well.
@re_direct.route('/testing', methods=['GET', 'POST'])
@roles_required()
def yudi_test():
    message=cipher_text()   # Get the ciptertext and randomiv
    # Want to put these into a string for Yudi's URL
    url_params = "&".join(["{k}={d}".format(k=k,d=d) for k,d in message.items()])
    return request
    #redirect("http://202.88.105.3/yudi/test5.php?{}".format(url_params), code=307)




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