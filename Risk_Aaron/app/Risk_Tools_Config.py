
# Function that would control all email sending notifications.
# email_flag, email_recipients = email_flag_fun("large_volume_alert")
def email_flag_fun(tool_name):
    data = {"large_volume_alert": (False,["aaron.lim@blackwellglobal.com"]),
            "scalping_comment" : (True, ["aaron.lim@blackwellglobal.com"]),
            "risk_autocut" : (True, "-"),    # Does not follow conventional email list. It has it's own if/else in the function
            }

    # Will return (Tru
    return data[tool_name.lower()] if tool_name.lower() in data else (False, False)