# The tableau URL all comes here.

def symbol_yesterday_Viz(symbol, book):
    url = """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    <div class='tableauPlaceholder' style='width: 2133px; height: 986px;'><object class='tableauViz' width='2133' height='986' style='display:none;'> 
    <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' /> 
    <param name='embed_code_version' value='3' /> 
    <param name='site_root' value='' /> 
    <param name='name' value='Yesterday_Trades_0617&#47;YESTERDAY' /> 
    <param name='tabs' value='yes' /> 
    <param name='toolbar' value='yes' /> 
    <param name='filter' value='Type~na={symbol}' /> 
    <param name='filter' value='BOOK3~na={book}' /> 
    <param name='showAppBanner' value='false' /></object></div> """.format(symbol=symbol.upper(), book=book.upper())
    return url


def Entity_Float_Viz(Entity):
    conversion = {'Alliance': 'Alliance', 'Alliance_A': 'Alliance (A)',    'ATG_A' : 'ATG (A)', 'BVE' : 'BVE', 'BVE_A' : 'BVE (A)',
    'BVI' : 'BVI', 'BVI_EGRet': 'BVI_EGRet' , 'BVI_EGRet_50%' :    'BVI_EGRet_50%',
    'BVI_EGRet_A' :    'BVI_EGRet (A)', 'BVI_Live1' :    'BVI_Live1',
    'BVI_Tiger' :    'BVI_Tiger', 'Cambodia' :    'Cambodia',
    'Cambodia_A' :    'Cambodia (A)', 'Cambodia_Tiger' :    'Cambodia_Tiger',
    'CN' :    'China',    'CN_A' :    'China (A)',    'Crypto' :    'Crypto',    'CY' :    'Cyprus',
     'CY_A' :    'Cyprus (A)',    'Dealing' :    'Dealing',
    'GH' :    'Ghana',    'HK' :    'Hong Kong',    'HK_A' :    'Hong Kong (A)',    'HK_Prop' :    'Hong Kong',
    'Indonesia' :    'Indonesia',    'LD4' :    'LD4',
     'MAM' :    'MAM',    'MCML' :    'MCML',    'MCML_A' :    'MCML (A)',    'Nigeria' :    'Nigeria',
    'NZ' :    'New Zealand',    'NZ_A' :    'New Zealand (A)',
    'Omnibus_sub' :    'Omnibus_sub',    'Philippines' :    'Philippines',
    'SG' :    'Singapore',    'SG_A' :    'Singapore (A)',    'TEST' :    'TEST',    'TW' :    'Taiwan',    'TW_A' :    'Taiwan (A)',    'TW_Tiger':    'TW_Tiger',
    'UK':    'United Kingdom',    'UK_A':    'United Kingdom (A)',
    'WhiteL_A':    'WhiteL (A)'}


    url = """<script type='text/javascript' src='https://202.88.105.5/javascripts/api/viz_v1.js'></script>
    <div class='tableauPlaceholder' style='width: 1600px; height: 950px;'><object class='tableauViz' width='1600' height='950' style='display:none;'>
    <param name='host_url' value='https%3A%2F%2F202.88.105.5%2F' />
    <param name='embed_code_version' value='3' />
    <param name='site_root' value='' />
    <param name='name' value='Aaron_Floating&#47;BGIFloating&#47;Aaron&#47;9c41c6b6-7cec-44cf-8099-ac8fb1e0ae2e' />
    <param name='tabs' value='yes' />
    <param name='toolbar' value='yes' />
    <param name='filter' value='Entity~na={Entity}' /> 
    <param name='showAppBanner' value='false' /></object></div>
    """.format(Entity=conversion[Entity] if Entity in conversion else Entity)

    return url