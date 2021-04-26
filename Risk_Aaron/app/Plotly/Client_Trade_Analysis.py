
import pandas as pd
from Helper_Flask_Lib import *
from unsync import unsync


# To return the open trades analysis.
# col is for larger tables, showing logins and such
# col_1 is for grouped 'group' tables,
# this one differs in that the dataframe is expected to have A/B Book.
def group_open_trades_analysis(df_open_trades, book, col, col_1, group=""):

    # Want the display line should be.
    display_line = group


    if len(df_open_trades) <= 0:    # If there are no closed trades for the day.
        return_df = pd.DataFrame([{"Note": "There are no open trades for {} now".format(display_line)}])
        return [return_df] * 5   # return 5 empty data frames.
        #[top_groups, bottom_groups, top_accounts , bottom_accounts, total_sum, largest_login , open_by_country]
    else:

        # Group the trades together.
        live_login_sum = df_open_trades.groupby(by=['LIVE', 'LOGIN', 'COUNTRY', 'GROUP']).sum().reset_index()

        #print(live_login_sum)
        # Round off the values that is not needed.
        live_login_sum["LOTS"] = round(live_login_sum['LOTS'],2)
        live_login_sum["NET_LOTS"] = round(live_login_sum['NET_LOTS'], 2)
        live_login_sum['REBATE'] = live_login_sum.apply(lambda x: color_rebate(rebate=x['REBATE'], pnl=x["CONVERTED_REVENUE"]), axis=1)
        live_login_sum["CONVERTED_REVENUE"] = round(live_login_sum['CONVERTED_REVENUE'], 2)
        #live_login_sum["PROFIT"] = round(live_login_sum['PROFIT'], 2)
        live_login_sum["PROFIT"] = live_login_sum["PROFIT"].apply(profit_red_green)
        live_login_sum["SWAPS"] = live_login_sum["SWAPS"].apply(profit_red_green)
            #round(live_login_sum['SWAPS'], 2)
        live_login_sum["LOGIN"] = live_login_sum.apply(lambda x: live_login_analysis_url(\
                                    Live=x['LIVE'].lower().replace("live", ""), Login=x["LOGIN"]), axis=1)
        live_login_sum["TOTAL_PROFIT"] = round(live_login_sum['TOTAL_PROFIT'], 2)


        # Want Top and winning accounts. If there are none. we will reflect accordingly.
        top_accounts = live_login_sum[live_login_sum['CONVERTED_REVENUE'] >= 0 ].sort_values('CONVERTED_REVENUE', ascending=False)[col].head(20)
        # Color the CONVERTED_REVENUE
        top_accounts["CONVERTED_REVENUE"] = top_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        top_accounts = pd.DataFrame([{"Comment": "There are currently no client with floating profit for {}".format(group)}]) \
                        if len(top_accounts) <= 0 else top_accounts



        # Want bottom and Loosing accounts. If there are none, we will reflect it accordingly.
        bottom_accounts = live_login_sum[live_login_sum['CONVERTED_REVENUE'] < 0 ].sort_values('CONVERTED_REVENUE', ascending=True)[col].head(20)
        # Color the CONVERTED_REVENUE
        bottom_accounts["CONVERTED_REVENUE"] = bottom_accounts["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        bottom_accounts = pd.DataFrame(
            [{"Comment": "There are currently no client with floating losses for {}".format(group)}]) \
            if len(bottom_accounts) <= 0 else bottom_accounts

        #
        # # By entities/Group
        # group_sum = df_open_trades.groupby(by=['COUNTRY', 'GROUP',])[['LOTS', 'NET_LOTS',
        #                                                               'CONVERTED_REVENUE', 'SYMBOL', 'REBATE',
        #                                                               'TOTAL_PROFIT']].sum().reset_index()
        #
        # # Want to color the rebate if profit <= 0, but Profit + rebate > 0
        # group_sum['REBATE'] = group_sum.apply(lambda x: color_rebate(rebate=x['REBATE'],
        #                                                 pnl=x["CONVERTED_REVENUE"]), axis=1)
        # # Round it off to be able to be printed better.
        # group_sum['CONVERTED_REVENUE'] = round(group_sum['CONVERTED_REVENUE'], 2)
        # group_sum['LOTS'] = round(group_sum['LOTS'], 2)
        # group_sum['NET_LOTS'] = round(group_sum['NET_LOTS'], 2)
        #
        #
        # # Only want those that are profitable
        # top_groups = group_sum[group_sum['CONVERTED_REVENUE']>=0].sort_values('CONVERTED_REVENUE',
        #                                                                       ascending=False)[col_1].head(20)
        # # Color the CONVERTED_REVENUE
        # top_groups["CONVERTED_REVENUE"] = top_groups["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))
        #
        # top_groups = pd.DataFrame([{"Comment": "There are currently no groups with floating profit for {}".format(symbol)}]) if \
        #     len(top_groups) <= 0 else top_groups
        #
        #
        # # Only want those that are making a loss
        # bottom_groups = group_sum[group_sum['CONVERTED_REVENUE']<=0].sort_values('CONVERTED_REVENUE', ascending=True)[col_1].head(20)
        # # Color the CONVERTED_REVENUE
        # bottom_groups["CONVERTED_REVENUE"] = bottom_groups["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))
        #
        # bottom_groups = pd.DataFrame(
        #     [{"Comment": "There are currently no groups with floating losses for {}".format(symbol)}]) if \
        #     len(bottom_groups) <= 0 else bottom_groups


        # Want to flip it based on A/B book, to reflect BGI side.
        df_open_trades["BGI_REVENUE"] = df_open_trades.apply(lambda x: x["CONVERTED_REVENUE"] * -1 if x["BOOK"].lower() != 'a' else x["CONVERTED_REVENUE"], axis=1)
        df_open_trades["BGI_PROFIT"] = df_open_trades.apply(
            lambda x: x["PROFIT"] * -1 if x["BOOK"].lower() != 'a' else x["PROFIT"], axis=1)
        df_open_trades["BGI_SWAPS"] = df_open_trades.apply(
            lambda x: x["SWAPS"] * -1 if x["BOOK"].lower() != 'a' else x["SWAPS"], axis=1)
        total_sum_Col = ['LOTS', 'BGI_REVENUE', 'BGI_PROFIT', 'BGI_SWAPS', 'REBATE']  # The columns that we want to show


        total_sum = df_open_trades[total_sum_Col].sum()
        #total_sum =  total_sum.apply(lambda x: round(x * -1 if book.lower() != 'a' else x, 2)) # Flip it to be on BGI Side if it's not a book.

        total_sum["LOTS"] = "{:,}".format(round(abs(total_sum["LOTS"]),2))  # Since it's Total lots, we only want the abs value
        total_sum['REBATE'] = color_rebate(rebate=total_sum['REBATE'], pnl=total_sum["BGI_REVENUE"])
        total_sum["BGI_PROFIT"] = profit_red_green(total_sum["BGI_PROFIT"])
        total_sum["BGI_SWAPS"] = profit_red_green(total_sum["BGI_SWAPS"])
        total_sum["BGI_REVENUE"] = profit_red_green(total_sum["BGI_REVENUE"])


        # Want the table by Symbol.
        open_by_country = df_open_trades.groupby(["SYMBOL"])[[ 'LOTS', 'NET_LOTS','PROFIT','CONVERTED_REVENUE', 'REBATE', 'TOTAL_PROFIT']].sum().reset_index()

        open_by_country["NET_LOTS"] = -1 * open_by_country["NET_LOTS"]

        open_by_country["REBATE"] = open_by_country.apply(lambda x: color_rebate(rebate=x['REBATE'],
                                                            pnl=x["CONVERTED_REVENUE"], multiplier=-1), axis=1)

        # Want to show BGI Side. Color according to BGI Side
        ## Flip it to be on BGI Side if it's not A book.
        open_by_country["TOTAL_PROFIT"] = open_by_country["TOTAL_PROFIT"].apply(lambda x: profit_red_green(x * -1) if \
                                                        book.lower() != "a" else profit_red_green(x))

        if book.lower() != "a": # Only want to flip sides when it's B book.
            open_by_country["CONVERTED_REVENUE"] = open_by_country["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(-1 * x))
        else:   # If it's A book. We don't need to do that.
            open_by_country["CONVERTED_REVENUE"] = open_by_country["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))


        open_by_country["LOTS"] = abs(open_by_country["LOTS"])
        # Flip it to be on BGI Side if it's not A book.
        open_by_country["PROFIT"] =  open_by_country["PROFIT"] * (-1 if book.lower() != 'a' else 1)
        open_by_country["PROFIT"] = open_by_country["PROFIT"].apply(profit_red_green)   # Color the Profit.


        open_by_country.sort_values(["NET_LOTS"], inplace=True)    # Sort it by Net_Lots
        open_by_country = open_by_country.head(20)                  # Want to take only the top 20

        open_by_country = pd.DataFrame(
            [{"Comment": "There are currently no Country with floating PnL for {}".format(group)}]) if \
            len(open_by_country) <= 0 else open_by_country


        # Largest (lots) Floating Account.
        largest_login = live_login_sum.sort_values('LOTS', ascending=False)[col].head(20)
        # Color the CONVERTED_REVENUE
        largest_login["CONVERTED_REVENUE"] = largest_login["CONVERTED_REVENUE"].apply(lambda x: profit_red_green(x))

        largest_login = pd.DataFrame(
            [{"Comment": "There are currently no login with open trades for {}".format(group)}]) if \
            len(largest_login) <= 0 else largest_login

    # top_groups, bottom_groups,
    #print(total_sum)
    return  [ top_accounts , bottom_accounts, total_sum, largest_login , open_by_country]


@unsync
# Live = 'live1' for example.
def Client_Group_Details(app, live, group):

    SQL_Query = """select count(*) as 'CLIENT_COUNT', SUM(BALANCE) as 'TOTAL_DEPOSIT',
    sum(mt4_users.CREDIT) AS 'TOTAL_CREDIT', SUM(EQUITY) AS 'TOTAL_EQUITY',
    SUM(CASE WHEN BALANCE > 0  THEN 1 ELSE 0 END) as 'CLIENT (BALANCE > 0)',
    SUM(CASE WHEN ENABLE_READONLY = 0  THEN 1 ELSE 0 END) as 'ACTIVE_CLIENT (READONLY = FALSE)',
    mt4_groups.CURRENCY, mt4_groups.MARGIN_CALL, mt4_groups.MARGIN_STOPOUT
    From {live}.mt4_users, {live}.mt4_groups
    where mt4_users.`group` like "{group}" AND mt4_users.`GROUP` = mt4_groups.`GROUP`
    """.format(live=live, group=group)
    #print(SQL_Query)
    ret_data = unsync_query_SQL_return_record(SQL_Query, app)

    col = ["TOTAL_DEPOSIT", "TOTAL_CREDIT", "TOTAL_EQUITY"]
    if len(ret_data) > 0:
        for c in col:
            if c in ret_data[0] and isfloat(ret_data[0][c]):
                ret_data[0][c] = profit_red_green(ret_data[0][c])

    return ret_data

@unsync
# Want to get consolidated PnL for the last 8 months.
def Client_Group_past_trades(app, live, group):

    hour_add_interval = 1 if live.lower() != "live5" else 0


    # SQL_Query = """select DATE_FORMAT( DATE_ADD(CLOSE_TIME, INTERVAL {hour} HOUR), "%Y-%m-01") as "CLOSE_MONTH",
    #   sum(VOLUME) * 0.01 as LOTS, ROUND(sum(SWAPS),2) as SWAPS, ROUND(sum(PROFIT),2) as PROFIT,
    #   SUM(symbol_rebate.rebate * VOLUME * 0.01) as REBATE
    # From {live}.mt4_trades, {live}.symbol_rebate
    # where `group` = "{group}" AND mt4_trades.SYMBOL = symbol_rebate.SYMBOL
    # and CLOSE_TIME >= DATE_SUB(DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 MONTH), "%Y-%m-01 00:00:00"), INTERVAL {hour} HOUR)
    # AND CMD < 2
    # GROUP BY `CLOSE_MONTH` """.format(live=live, group=group, hour=hour_add_interval)



    SQL_Query = """SELECT CLOSE_MONTH, SUM(LOTS) AS LOTS, SUM(SWAPS) AS SWAPS, SUM(PROFIT) AS PROFIT, SUM(symbol_rebate.REBATE * LOTS) AS REBATE FROM (
    select DATE_FORMAT( DATE_ADD(CLOSE_TIME, INTERVAL {hour} HOUR), "%Y-%m-01 00:00:00") as "CLOSE_MONTH",
          sum(VOLUME) * 0.01 as LOTS, ROUND(sum(SWAPS),2) as SWAPS, ROUND(sum(PROFIT),2) as PROFIT,
            SYMBOL
        From {live}.mt4_trades
        where `group` like "{group}"
        and CLOSE_TIME >= DATE_SUB(DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MONTH), "%Y-%m-01 00:00:00"), INTERVAL {hour} HOUR)
        AND CMD < 2
        GROUP BY `CLOSE_MONTH`, SYMBOL) AS A, {live}.symbol_rebate
        WHERE A.SYMBOL = symbol_rebate.SYMBOL
    GROUP BY A.`CLOSE_MONTH`""".format(live=live, group=group, hour=hour_add_interval)

    # SQL_Query = """SELECT CLOSE_MONTH, SUM(LOTS) AS LOTS, SUM(SWAPS) AS SWAPS, SUM(PROFIT) AS PROFIT FROM (
    # select DATE_FORMAT( DATE_ADD(CLOSE_TIME, INTERVAL {hour} HOUR), "%Y-%m-01 00:00:00") as "CLOSE_MONTH",
    #       sum(VOLUME) * 0.01 as LOTS, ROUND(sum(SWAPS),2) as SWAPS, ROUND(sum(PROFIT),2) as PROFIT,
    #         SYMBOL
    #     From {live}.mt4_trades
    #     where `group` = "{group}"
    #     and CLOSE_TIME >= DATE_SUB(DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 2 MONTH), "%Y-%m-01 00:00:00"), INTERVAL {hour} HOUR)
    #     AND CMD < 2
    #     GROUP BY `CLOSE_MONTH`, SYMBOL) AS A
    # GROUP BY A.`CLOSE_MONTH`""".format(live=live, group=group, hour=hour_add_interval)


    #print(SQL_Query)

    ret_data = unsync_query_SQL_return_record(SQL_Query, app)
    df = pd.DataFrame(ret_data)
    #print(ret_data)
    if len(ret_data) > 0:
        df["TOTAL"] =  df["PROFIT"] +  df["SWAPS"] +  df["REBATE"]
        df["SWAPS"] = df["SWAPS"].apply(profit_red_green)
        df["PROFIT"] = df["PROFIT"].apply(profit_red_green)
        #df["TOTAL"] = df["TOTAL"].apply(profit_red_green)
        #df["REBATE"] = df["REBATE"].apply(profit_red_green)

        # Want to try and print the date(Month) nicely
        df["CLOSE_MONTH"] = pd.to_datetime(df["CLOSE_MONTH"])
        df.sort_values(by=["CLOSE_MONTH"], ascending=False, inplace=True)
        df["CLOSE_MONTH"] =  df["CLOSE_MONTH"].apply(lambda x: x.strftime('%B %Y'))


        df = df[["CLOSE_MONTH", "LOTS", "SWAPS", "PROFIT", "REBATE",  "TOTAL"]] # Order the column "REBATE",  "TOTAL"
        #pd.to_datetime(df.columns, format='%b %y')

        #df.sort_values(by=["YEAR"])
        #print(df)
    return df
