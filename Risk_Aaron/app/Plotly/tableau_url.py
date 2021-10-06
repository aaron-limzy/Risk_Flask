from flask import  Markup

# The tableau URL all comes here.

def symbol_yesterday_Viz(symbol, book):
    url = """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    <div class='tableauPlaceholder' style='width: 2133px; height: 986px;'><object class='tableauViz' width='2133' height='986' style='display:none;'> 
    <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' /> 
    <param name='embed_code_version' value='3' /> 
    <param name='site_root' value='' /> 
    <param name='name' value='Yesterday_Trades_0617&#47;YESTERDAY' /> 
    <param name='tabs' value='no' /> 
    <param name='toolbar' value='yes' /> 
    <param name='filter' value='Type~na={symbol}' /> 
    <param name='filter' value='BOOK3~na={book}' /> 
    <param name='showAppBanner' value='false' /></object></div> """.format(symbol=symbol.upper(), book=book.upper())
    return Markup(url)


def Entity_Float_Viz(Entity="", Symbol=""):

    # The viz has it with the proper entity name.
    # So we need to do some conversion here.
    conversion = {'alliance': 'Alliance',
     'alliance_a': 'Alliance (A)', 'atg_a': 'ATG (A)', 'bve': 'BVE',
     'bve_a': 'BVE (A)', 'bvi': 'BVI', 'bvi_egret': 'BVI_EGRet',
     'bvi_egret_50%': 'BVI_EGRet_50%', 'bvi_egret_a': 'BVI_EGRet (A)', 'bvi_live1': 'BVI_Live1',
     'bvi_tiger': 'BVI_Tiger', 'cambodia': 'Cambodia', 'cambodia_a': 'Cambodia (A)',
     'cambodia_tiger': 'Cambodia_Tiger', 'cn': 'China', 'cn_a': 'China (A)',
     'crypto': 'Crypto', 'cy': 'Cyprus', 'cy_a': 'Cyprus (A)', 'dealing': 'Dealing',
     'gh': 'Ghana', 'hk': 'Hong Kong', 'hk_a': 'Hong Kong (A)',
     'hk_prop': 'Hong Kong', 'indonesia': 'Indonesia',
     'ld4': 'LD4', 'mam': 'MAM', 'mcml': 'MCML',
     'mcml_a': 'MCML (A)',
     'nigeria': 'Nigeria', 'nz': 'New Zealand', 'nz_a': 'New Zealand (A)',
     'omnibus_sub': 'Omnibus_sub', 'philippines': 'Philippines', 'sg': 'Singapore',
     'sg_a': 'Singapore (A)', 'test': 'TEST', 'tw': 'Taiwan', 'tw_a': 'Taiwan (A)',
     'tw_tiger': 'TW_Tiger', 'uk': 'United Kingdom',
     'uk_a': 'United Kingdom (A)', 'whitel_a': 'WhiteL (A)'}


    symbol_param = "<param name='filter' value='SYMBOL~na={symbol}' /> ".format(symbol=Symbol.upper()) if Symbol != "" else ""

    entity_param = "<param name='filter' value='Entity~na={Entity}' /> ".format(Entity=conversion[Entity.lower()] \
        if Entity.lower() in conversion else Entity) if Entity != "" else ""

    url = """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    <div class='tableauPlaceholder' style='width: 1600px; height: 950px;'><object class='tableauViz' width='1600' height='950' style='display:none;'>
    <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' />
    <param name='embed_code_version' value='3' />
    <param name='site_root' value='' />
    <param name='name' value='Aaron_Floating&#47;BGIFloating&#47;Aaron&#47;9c41c6b6-7cec-44cf-8099-ac8fb1e0ae2e' />
    <param name='tabs' value='no' />
    <param name='toolbar' value='yes' />
    {symbol_param} 
    {entity_param}
    <param name='showAppBanner' value='false' /></object></div>
    """.format(symbol_param=symbol_param, entity_param=entity_param)

    return Markup(url)


# yudi's viz on Tableau.
def individual_client_analysis(live, login):

    live_param = "<param name='filter' value='LIVEP={live}' /> ".format(live=live)
    login_param = "<param name='filter' value='LOGINP={login}' /> ".format(login=login)


    url = """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    <div class='tableauPlaceholder' style='width: 1654px; height: 3534px;'><object class='tableauViz' width='1654' height='3534' style='display:none;'>
    <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' />
    <param name='embed_code_version' value='3' />
    <param name='site_root' value='' />
    <param name='name' value='ClientAnalysis&#47;Dashboard1' />
    <param name='tabs' value='no' />
    <param name='toolbar' value='yes' />
    <param name='showAppBanner' value='false' />
    <param name='device' value='desktop' />
    {live_param} {login_param}
    <param name='original_view' value='yes' /></object></div>""".format(live_param=live_param,login_param=login_param)

    return Markup(url)



# Joyce did this.
# URL from 5th Jan 2021
def symbol_float_tableau():
    # return """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    # <div class='tableauPlaceholder' style='width: 1680px; height: 1350px;'><object class='tableauViz' width='1680' height='1350' style='display:none;'>
    # <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' />
    # <param name='embed_code_version' value='3' />
    # <param name='site_root' value='' />
    # <param name='name' value='SymbolFloatAll&#47;SymbolFloatVolume' />
    # <param name='tabs' value='yes' />
    # <param name='toolbar' value='yes' />
    # <param name='showAppBanner' value='false' /></object></div>"""

    url = "https://202.88.105.5/views/FEI_MT5Floating/MT5FloatingB"

    #url = "https://192.168.64.56/views/FEI_MT5Floating/MT5FloatingB?:showAppBanner=false&:display_count=n&:showVizHome=n&:origin=viz_share_link"
    #url = "https://202.88.105.5/views/SymbolFloatAll/SymbolFloatVolume?:showAppBanner=false&:display_count=n&:showVizHome=n&:origin=viz_share_link"
    height = 1350
    width = 1680
    #return [url, height, width]
    return url