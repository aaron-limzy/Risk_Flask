# For the background for some of the websites.


def background_pic(website):


    default = ""
    gb_return = {"save_BGI_float" : "css/city_overview.jpg",
     }


    return_val = gb_return[website] if website in gb_return else default


    return return_val