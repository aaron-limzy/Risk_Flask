

def mt5_b_book_query(time_diff=0):
    time_diff_mt5 = time_diff if time_diff != 0 else '(SELECT result FROM aaron.`aaron_misc_data` where item = "mt5_timing_diff")'


    sql_query = r"""SELECT FinalTable.Country,FinalTable.BaseSymbol,-NetVolume AS NetVolume,FloatVolume,AverageFloatVolume AS DailyAverage, -FloatProfit AS FloatProfit, TodayVolume AS TodayClosedVolume,-TodayProfitUsd AS TodayClosedProfitUsd,YesterdayVolume AS YesterdayClosedVolume,
            -YesterdayProfitUsd AS YesterdayClosedProfitUsd FROM(
                
            SELECT F.Country,F.BaseSymbol,sum(NetVolume) AS NetVolume,SUM(FloatVolume) AS FloatVolume, SUM(FloatProfit) AS FloatProfit,
            SUM(COALESCE(Volume,0)) as TodayVolume,SUM(COALESCE(ROUND(TodayProfitUsd,2),0)) AS TodayProfitUsd,SUM(COALESCE(YesterdayVolume,0)) AS YesterdayVolume,
            SUM(COALESCE(ROUND(YesterdayProfitUsd,2),0)) AS YesterdayProfitUsd FROM
                    (#FLOAT MT5 live 1 live 2
                        SELECT LEFT(RIGHT(`Group`,length(`group`)-5),locate("\\",`Group`,6)-6) AS Country,`Group`,BaseSymbol, SUM(BuyVolume) - SUM(SellVolume) AS NetVolume,
                        SUM(Volume) AS FloatVolume,SUM(Profit_usd) AS FloatProfit FROM
                            (#FLOAT MT5 live 1
                                select t1.*,t2.`Group`,t3.Currency, CASE 
                                WHEN t3.Currency = "USD" THEN Profit
                                WHEN LEFT(t3.SYMBOL,3)="USD" THEN Profit / ((AskLast+BidLast)/2)
                                ELSE Profit * ((AskLast+BidLast)/2)
                                END AS Profit_usd, SymbolPath,BaseSymbol
                                from#current open position with conversion rate
                                    (select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
                                    IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5.mt5_positions WHERE LENGTH(Login)>4 )t1
                                    LEFT JOIN
                                    (SELECT Login, `Group` FROM mt5.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
                                    on t1.Login = t2.Login
                                    LEFT JOIN
                                    (SELECT * FROM mt5.group_conversion_rate WHERE `Group` like "real%")t3 
                                    ON t2.`Group` = t3.`Group`
                                    LEFT JOIN
                                    (SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5.yudi_BaseSymbol)t4
                                    on t1.Symbol = t4.Symbol
        
                                UNION ALL
                                #FLOAT MT5 live 2
                                select t1.*,t2.`Group`,t3.Currency, CASE 
                                WHEN t3.Currency = "USD" THEN Profit
                                WHEN LEFT(t3.SYMBOL,3)="USD" THEN Profit / ((AskLast+BidLast)/2)
                                ELSE Profit * ((AskLast+BidLast)/2)
                                END AS Profit_usd, SymbolPath,BaseSymbol
                                from#current open position select * from mt5.mt5_positions
                                    (select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
                                    IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5_uk.mt5_positions WHERE LENGTH(Login)>4 )t1
                                    LEFT JOIN
                                    (SELECT Login, `Group` FROM mt5_uk.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
                                    on t1.Login = t2.Login
                                    LEFT JOIN
                                    (SELECT * FROM mt5_uk.group_conversion_rate WHERE `Group` like "real%")t3
                                    ON t2.`Group` = t3.`Group`
                                    LEFT JOIN
                                    (SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t4
                                    on t1.Symbol = t4.Symbol
                        )FloatTable
                        WHERE `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                        GROUP BY `Group`,BaseSymbol
                        
                    )F
                    LEFT JOIN 
                    (#today closed for live 1 and live 2
                        SELECT `Group`,BaseSymbol,SUM(Volume) AS Volume,SUM(TodayProfitUsd) AS TodayProfitUsd,SUM(MarkupRebate) AS Rebate FROM
                        (
                            ##today closed for live 1
                            SELECT t3.`Group`,t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
                                (SELECT t1.`Group`, t1.Symbol, MarkupRebate, Volume, CASE
                                WHEN Currency ="USD" THEN `Storage`+ Profit
                                WHEN LEFT(t2.Symbol,3) = "USD" THEN (`Storage`+ Profit)/((BidLast+AskLast)/2)
                                ELSE (`Storage`+ Profit)*((BidLast+AskLast)/2)
                                END AS TodayProfitUsd FROM(
                                    SELECT `Group`,Symbol, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
                                    SUM(Profit) AS Profit FROM mt5.mt5_deals 
                                    WHERE  Action < 2  AND (Entry = 1  OR (mt5_deals.Entry > 1 AND mt5_deals.PricePosition <> 0))  AND 
                                    Time>= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                                    GROUP BY `Group`,Symbol
                                    )t1
                                    LEFT JOIN
                                        (SELECT * FROM mt5.group_conversion_rate)t2
                                    ON t1.`Group` = t2.`Group`
                        
                                )t3
                                LEFT JOIN 
                                ( SELECT Symbol, BaseSymbol FROM mt5.yudi_basesymbol)t5
                                ON t3.Symbol = t5.Symbol 
        
                            UNION ALL 
                            ##today closed for live 1
                
                            SELECT t3.`Group`,t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
                                    (SELECT t1.`Group`,t1.Symbol, MarkupRebate, Volume, CASE
                                    WHEN Currency ="USD" THEN SUM(`Storage`+ Profit)
                                    WHEN LEFT(t2.Symbol,3) = "USD" THEN (`Storage`+ Profit)/((BidLast+AskLast)/2)
                                    ELSE (`Storage`+ Profit)*((BidLast+AskLast)/2)
                                    END AS TodayProfitUsd FROM 
                                        (SELECT Symbol,`Group`, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
                                        SUM(Profit) AS Profit FROM mt5_uk.mt5_deals 
                                        WHERE  Action < 2 AND (Entry = 1  OR (mt5_uk.mt5_deals .Entry > 1 AND mt5_uk.mt5_deals.PricePosition <> 0))  AND 
                                        Time>= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                                        GROUP BY `Group`,Symbol
                                        )t1
                                        LEFT JOIN
                                            (SELECT * FROM mt5_uk.group_conversion_rate)t2
                                        ON t1.`Group` = t2.`Group`
                                        
                                )t3
                                LEFT JOIN 
                                ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t5
                                ON t3.Symbol = t5.Symbol 
                        )Z
                        GROUP BY `Group`,BaseSymbol
                    )TodayClosed
                    ON F.BaseSymbol = TodayClosed.BaseSymbol AND F.`Group` = TodayClosed.`Group`
                
                    LEFT JOIN
                    (	#yestClosed
                        SELECT `Group`,BaseSymbol,SUM(YesterdayVolume) AS YesterdayVolume, SUM(YesterdayProfit_usd) AS YesterdayProfitUsd,
                        SUM(YesterdayMarkupRebate) AS YesterdayRebate 
                        FROM
                            #yestClosed live 1
                            (SELECT `Group`,yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
                                (SELECT `Group`,Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
                                FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%" 
                                AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
                                GROUP BY `Group`,Symbol
                                )yestTable
                                LEFT JOIN 
                                ( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)yt2
                                ON yestTable.Symbol = yt2.Symbol 
                            UNION ALL
                            #yestClosed live 2
                            SELECT `Group`,yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
                                (SELECT `Group`,Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
                                FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%" 
                                AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
                                GROUP BY `Group`,Symbol
                                )yestTable
                                LEFT JOIN 
                                ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)yt2
                                ON yestTable.Symbol = yt2.Symbol 
                                
                        )z2
                        GROUP BY `Group`,BaseSymbol
                    )YesterdayClosed
                    ON F.BaseSymbol = YesterdayClosed.BaseSymbol AND F.`Group` = YesterdayClosed.`Group`
                GROUP BY Country,BaseSymbol
        )FinalTable
            
        LEFT JOIN 
        (#getting average Float
            SELECT LEFT(RIGHT(`Group`,length(`group`)-5),locate("\\",`Group`,6)-6) AS Country,`Group`,basesymbol,ROUND(AVG(FloatVolume),2) AverageFloatVolume from(
                SELECT date,`Group`,ft.Symbol,BaseSymbol,FloatVolume from(
                    SELECT date,`Group`,symbol,FloatVolume FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR)- INTERVAL 1 MONTH 
                    AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                )ft
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)ft2
                ON ft.Symbol = ft2.Symbol
                
                UNION ALL
                
                SELECT date,`Group`,ft.Symbol,BaseSymbol,FloatVolume from(
                    SELECT date,`Group`,symbol,FloatVolume FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR)- INTERVAL 1 MONTH 
                    AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                )ft
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)ft2
                ON ft.Symbol = ft2.Symbol
            )Z
            GROUP BY Country,basesymbol
        )AverageVolumet
        ON AverageVolumet.BaseSymbol = FinalTable.BaseSymbol AND AverageVolumet.Country = FinalTable.Country""".format(time_diff_mt5=time_diff_mt5)


    # print(sql_query)
    # print()
    # print()
    # print()
    # print()
    return sql_query



# Will select and insert for B Book MT5
def mt5_BBook_select_insert(time_diff=0):

    time_diff_mt5 = time_diff if time_diff != 0 else '(SELECT result FROM aaron.`aaron_misc_data` where item = "mt5_timing_diff")'

    sql_statement = r"""INSERT INTO aaron.`bgi_mt5_float_save` (entity, symbol, net_floating_volume, floating_volume, floating_revenue,closed_vol_today, closed_revenue_today, datetime)
                SELECT FinalTable.Country,FinalTable.BaseSymbol,-NetVolume AS NetVolume,FloatVolume, -FloatProfit AS FloatProfit, TodayVolume AS TodayClosedVolume,-TodayProfitUsd AS TodayClosedProfitUsd, now() as datetime FROM(
                SELECT F.Country,F.BaseSymbol,sum(NetVolume) AS NetVolume,SUM(FloatVolume) AS FloatVolume, SUM(FloatProfit) AS FloatProfit,
                SUM(COALESCE(Volume,0)) as TodayVolume,SUM(COALESCE(ROUND(TodayProfitUsd,2),0)) AS TodayProfitUsd FROM
                                (#FLOAT MT5 live 1 live 2
                                        SELECT LEFT(RIGHT(`Group`,length(`group`)-5),locate("\\",`Group`,6)-6) AS Country,`Group`,BaseSymbol, SUM(BuyVolume) - SUM(SellVolume) AS NetVolume,
                                        SUM(Volume) AS FloatVolume,SUM(Profit_usd) AS FloatProfit FROM
                                                (#FLOAT MT5 live 1
                                                        select t1.*,t2.`Group`,t3.Currency, CASE 
                                                        WHEN t3.Currency = "USD" THEN Profit
                                                        WHEN LEFT(t3.SYMBOL,3)="USD" THEN (Profit / ((AskLast+BidLast)/2))
                                                        ELSE Profit * ((AskLast+BidLast)/2)
                                                        END AS Profit_usd, SymbolPath,BaseSymbol
                                                        from#current open position with conversion rate
                                                                (select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
                                                                IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5.mt5_positions WHERE LENGTH(Login)>4 )t1
                                                                LEFT JOIN
                                                                (SELECT Login, `Group` FROM mt5.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
                                                                on t1.Login = t2.Login
                                                                LEFT JOIN
                                                                (SELECT * FROM mt5.group_conversion_rate WHERE `Group` like "real%")t3 
                                                                ON t2.`Group` = t3.`Group`
                                                                LEFT JOIN
                                                                (SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5.yudi_BaseSymbol)t4
                                                                on t1.Symbol = t4.Symbol
                
                                                        UNION ALL
                                                        #FLOAT MT5 live 2
                                                        select t1.*,t2.`Group`,t3.Currency, CASE 
                                                        WHEN t3.Currency = "USD" THEN Profit
                                                        WHEN LEFT(t3.SYMBOL,3)="USD" THEN Profit / ((AskLast+BidLast)/2)
                                                        ELSE Profit * ((AskLast+BidLast)/2)
                                                        END AS Profit_usd, SymbolPath,BaseSymbol
                                                        from#current open position select * from mt5.mt5_positions
                                                                (select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
                                                                IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5_uk.mt5_positions WHERE LENGTH(Login)>4 )t1
                                                                LEFT JOIN
                                                                (SELECT Login, `Group` FROM mt5_uk.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
                                                                on t1.Login = t2.Login
                                                                LEFT JOIN
                                                                (SELECT * FROM mt5_uk.group_conversion_rate WHERE `Group` like "real%")t3
                                                                ON t2.`Group` = t3.`Group`
                                                                LEFT JOIN
                                                                (SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t4
                                                                on t1.Symbol = t4.Symbol
                                        )FloatTable
                                        WHERE `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                                        GROUP BY `Group`,BaseSymbol
                                        
                                )F
                                LEFT JOIN 
                                (#today closed for live 1 and live 2
                                        SELECT `Group`,BaseSymbol,SUM(Volume) AS Volume,SUM(TodayProfitUsd) AS TodayProfitUsd,SUM(MarkupRebate) AS Rebate FROM
                                        (
                                                ##today closed for live 1
                                                SELECT t3.`Group`,t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
                                                        (SELECT t1.`Group`, t1.Symbol, MarkupRebate, Volume, CASE
                                                        WHEN Currency ="USD" THEN `Storage`+ Profit
                                                        WHEN LEFT(t2.Symbol,3) = "USD" THEN (`Storage`+ Profit)/((BidLast+AskLast)/2)
                                                        ELSE (`Storage`+ Profit)*((BidLast+AskLast)/2)
                                                        END AS TodayProfitUsd FROM(
                                                                SELECT `Group`,Symbol, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
                                                                SUM(Profit) AS Profit FROM mt5.mt5_deals 
                                                                WHERE  Action < 2  AND (Entry = 1  OR (mt5_deals.Entry > 1 AND mt5_deals.PricePosition <> 0))  AND 
                                                                Time>= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                                                                GROUP BY `Group`,Symbol
                                                                )t1
                                                                LEFT JOIN
                                                                        (SELECT * FROM mt5.group_conversion_rate)t2
                                                                ON t1.`Group` = t2.`Group`
                                        
                                                        )t3
                                                        LEFT JOIN 
                                                        ( SELECT Symbol, BaseSymbol FROM mt5.yudi_basesymbol)t5
                                                        ON t3.Symbol = t5.Symbol 
                
                                                UNION ALL 
                                                ##today closed for live 1
                        
                                                SELECT t3.`Group`,t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
                                                                (SELECT t1.`Group`,t1.Symbol, MarkupRebate, Volume, CASE
                                                                WHEN Currency ="USD" THEN SUM(`Storage`+ Profit)
                                                                WHEN LEFT(t2.Symbol,3) = "USD" THEN (`Storage`+ Profit)/((BidLast+AskLast)/2)
                                                                ELSE (`Storage`+ Profit)*((BidLast+AskLast)/2)
                                                                END AS TodayProfitUsd FROM 
                                                                        (SELECT Symbol,`Group`, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
                                                                        SUM(Profit) AS Profit FROM mt5_uk.mt5_deals 
                                                                        WHERE  Action < 2 AND (Entry = 1  OR (mt5_uk.mt5_deals .Entry > 1 AND mt5_uk.mt5_deals.PricePosition <> 0))  AND 
                                                                        Time>= DATE(NOW() - INTERVAL {time_diff_mt5} HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                                                                        GROUP BY `Group`,Symbol
                                                                        )t1
                                                                        LEFT JOIN
                                                                                (SELECT * FROM mt5_uk.group_conversion_rate)t2
                                                                        ON t1.`Group` = t2.`Group`
                                                                        
                                                        )t3
                                                        LEFT JOIN 
                                                        ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t5
                                                        ON t3.Symbol = t5.Symbol 
                                        )Z
                                        GROUP BY `Group`,BaseSymbol
                                )TodayClosed
                                ON F.BaseSymbol = TodayClosed.BaseSymbol AND F.`Group` = TodayClosed.`Group`
                        GROUP BY Country,BaseSymbol
                )FinalTable ON DUPLICATE KEY UPDATE `net_floating_volume` = VALUES(net_floating_volume), `floating_volume` = VALUES(floating_volume),  `floating_revenue` = VALUES(floating_revenue)
                """.format(time_diff_mt5=time_diff_mt5)

    return sql_statement


# Want to use to get MT5 yesterday's PnL by symbol
# BGI Side
def mt5_symbol_yesterday_pnl_query():
    sql_query = r"""SELECT BaseSymbol as `SYMBOL`, SUM(YesterdayVolume) AS YesterdayVolume, -1 * SUM(YesterdayProfit_usd) AS YesterdayProfitUsd,
        -1 * SUM(YesterdayMarkupRebate) AS YesterdayRebate, now() as YESTERDAY_DATETIME_PULL
        FROM
            #yestClosed live 1
            (SELECT `Group`,yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
                (SELECT `Group`,Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
                FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%" 
                AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
                GROUP BY `Group`,Symbol
                )yestTable
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)yt2
                ON yestTable.Symbol = yt2.Symbol 
            UNION ALL
            #yestClosed live 2
            SELECT `Group`,yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
                (SELECT `Group`,Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
                FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%" 
                AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
                GROUP BY `Group`,Symbol
                )yestTable
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)yt2
                ON yestTable.Symbol = yt2.Symbol 
        )z2 group by BaseSymbol """
    return sql_query


# Want to get MT5 Futures LP Account details
# Remove MARKET EQUITY as it is the same as EQUITY
# Remoced CANDRAW as it's the same as EQUITY
def mt5_futures_LP_details_query():
    sql_query = r"""SELECT 'e800941' as ACCOUNT, CURRENCY, BALANCE, EQUITY, ACCTINITIALMARGIN, ACCTMAINTENANCEMARGIN, FROZENFEE, DATETIME  
        FROM e800941.`esunny_account`
        UNION
        SELECT  'e91220008' as ACCOUNT, CURRENCY, BALANCE, EQUITY,  ACCTINITIALMARGIN, ACCTMAINTENANCEMARGIN, FROZENFEE, DATETIME  
        FROM e91220008.`esunny_account`"""
    return sql_query

# Using Procedure on MYSQL to save the code.
def mt5_ABook_query():
    sql_query = r"""call aaron.mt5_ABook_2()"""
    return sql_query
