def mt5_b_book_query(time_diff=0):
    time_diff_mt5 = time_diff if time_diff != 0 else '(SELECT result FROM aaron.`aaron_misc_data` where item = "mt5_timing_diff")'

#     sql_query = """
# SELECT F.BaseSymbol,-NetVolume AS NetVolume,FloatVolume,AverageFloatVolume AS DailyAverage, -FloatProfit AS FloatProfit,COALESCE(Volume,0) as TodayVolume,-COALESCE(ROUND(TodayProfitUsd,2),0) AS TodayProfitUsd,COALESCE(YesterdayVolume,0) AS YesterdayVolume,
# 	-COALESCE(ROUND(YesterdayProfitUsd,2),0) AS YesterdayProfitUsd FROM
# 		(#FLOAT MT5 live 1 live 2
# 			SELECT BaseSymbol, SUM(BuyVolume) - SUM(SellVolume) AS NetVolume,SUM(Volume) AS FloatVolume,SUM(Profit_usd) AS FloatProfit FROM
# 				(#FLOAT MT5 live 1
# 					select t1.*,t2.`Group`,t3.Currency, CASE
# 					WHEN t3.Currency = "USD" THEN Profit
# 					WHEN LEFT(t3.SYMBOL,3)="USD" THEN Profit / ((AskLast+BidLast)/2)
# 					ELSE Profit * ((AskLast+BidLast)/2)
# 					END AS Profit_usd, SymbolPath,BaseSymbol
# 					from#current open position with conversion rate
# 						(select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
# 						IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5.mt5_positions WHERE LENGTH(Login)>4 )t1
# 						LEFT JOIN
# 						(SELECT Login, `Group` FROM mt5.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
# 						on t1.Login = t2.Login
# 						LEFT JOIN
# 						(SELECT * FROM mt5.group_conversion_rate WHERE `Group` like "real%")t3
# 						ON t2.`Group` = t3.`Group`
# 						LEFT JOIN
# 						(SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5.yudi_BaseSymbol)t4
# 						on t1.Symbol = t4.Symbol
#
# 					UNION ALL
# 					#FLOAT MT5 live 2
# 					select t1.*,t2.`Group`,t3.Currency, CASE
# 					WHEN t3.Currency = "USD" THEN Profit
# 					WHEN LEFT(t3.SYMBOL,3)="USD" THEN Profit / ((AskLast+BidLast)/2)
# 					ELSE Profit * ((AskLast+BidLast)/2)
# 					END AS Profit_usd, SymbolPath,BaseSymbol
# 					from#current open position select * from mt5.mt5_positions
# 						(select Position, Login, Symbol, Action,ContractSize, TimeUpdate,Volume/10000 AS Volume,IF(Action =0,Volume/10000,0) AS BuyVolume,
# 						IF(Action = 1,Volume/10000,0) AS SellVolume, `Storage`+Profit AS Profit from mt5_uk.mt5_positions WHERE LENGTH(Login)>4 )t1
# 						LEFT JOIN
# 						(SELECT Login, `Group` FROM mt5_uk.mt5_users WHERE `Group` like "real%" and LENGTH(Login)>4)t2
# 						on t1.Login = t2.Login
# 						LEFT JOIN
# 						(SELECT * FROM mt5_uk.group_conversion_rate WHERE `Group` like "real%")t3
# 						ON t2.`Group` = t3.`Group`
# 						LEFT JOIN
# 						(SELECT Symbol,  SymbolPath,BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t4
# 						on t1.Symbol = t4.Symbol
# 			)FloatTable
# 			WHERE `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
# 			GROUP BY BaseSymbol
#
# 		)F
# 		LEFT JOIN
# 		(#today closed for live 1 and live 2
# 			SELECT BaseSymbol,SUM(Volume) AS Volume,SUM(TodayProfitUsd) AS TodayProfitUsd,SUM(MarkupRebate) AS Rebate FROM
# 			(
# 				##today closed for live 1
# 				SELECT t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
# 					(SELECT t1.Symbol, sum(MarkupRebate) AS MarkupRebate, Sum(Volume) AS Volume, CASE
# 					WHEN Currency ="USD" THEN SUM(`Storage`+ Profit)
# 					WHEN LEFT(t2.Symbol,3) = "USD" THEN SUM(`Storage`+ Profit)/((BidLast+AskLast)/2)
# 					ELSE SUM(`Storage`+ Profit)*((BidLast+AskLast)/2)
# 					END AS TodayProfitUsd FROM(
# 						SELECT Symbol,`Group`, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
# 						SUM(Profit) AS Profit FROM mt5.mt5_deals
# 						WHERE  Action < 2  AND (Entry = 1  OR (mt5_deals.Entry > 1 AND mt5_deals.PricePosition <> 0))  AND
# 						Time>= DATE(NOW() - INTERVAL 7 HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
# 						GROUP BY Symbol,`Group`
# 						)t1
# 						LEFT JOIN
# 							(SELECT * FROM mt5.group_conversion_rate)t2
# 						ON t1.`Group` = t2.`Group`
# 						GROUP BY Symbol
# 					)t3
# 					LEFT JOIN
# 					( SELECT Symbol, BaseSymbol FROM mt5.yudi_basesymbol)t5
# 					ON t3.Symbol = t5.Symbol
#
# 				UNION ALL
# 				##today closed for live 1
#
# 				SELECT t3.Symbol,MarkupRebate,Volume,TodayProfitUsd,BaseSymbol FROM
# 						(SELECT t1.Symbol, sum(MarkupRebate) AS MarkupRebate, Sum(Volume) AS Volume, CASE
# 						WHEN Currency ="USD" THEN SUM(`Storage`+ Profit)
# 						WHEN LEFT(t2.Symbol,3) = "USD" THEN SUM(`Storage`+ Profit)/((BidLast+AskLast)/2)
# 						ELSE SUM(`Storage`+ Profit)*((BidLast+AskLast)/2)
# 						END AS TodayProfitUsd FROM
# 							(SELECT Symbol,`Group`, SUM(SpreadDiff) * sum(Volume)/10000 AS MarkupRebate, Sum(Volume)/10000 As Volume, SUM(`Storage`) AS `Storage`,
# 							SUM(Profit) AS Profit FROM mt5_uk.mt5_deals
# 							WHERE  Action < 2 AND (Entry = 1  OR (mt5_uk.mt5_deals .Entry > 1 AND mt5_uk.mt5_deals.PricePosition <> 0))  AND
# 							Time>= DATE(NOW() - INTERVAL 7 HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
# 							GROUP BY Symbol,`Group`
# 							)t1
# 							LEFT JOIN
# 								(SELECT * FROM mt5_uk.group_conversion_rate)t2
# 							ON t1.`Group` = t2.`Group`
# 							GROUP BY Symbol
# 					)t3
# 					LEFT JOIN
# 					( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)t5
# 					ON t3.Symbol = t5.Symbol
# 			)Z
# 			GROUP BY BaseSymbol
# 		)TodayClosed
# 		ON F.BaseSymbol = TodayClosed.BaseSymbol
#
# 		LEFT JOIN
# 		(	#yestClosed
# 			SELECT BaseSymbol,SUM(YesterdayVolume) AS YesterdayVolume, SUM(YesterdayProfit_usd) AS YesterdayProfitUsd,
# 			SUM(YesterdayMarkupRebate) AS YesterdayRebate
# 			FROM
# 				#yestClosed live 1
# 				(SELECT yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
# 					(SELECT Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
# 					FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%"
# 					AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
# 					GROUP BY Symbol
# 					)yestTable
# 					LEFT JOIN
# 					( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)yt2
# 					ON yestTable.Symbol = yt2.Symbol
# 				UNION ALL
# 				#yestClosed live 2
# 				SELECT yestTable.Symbol,YesterdayVolume,YesterdayProfit_usd,YesterdayMarkupRebate,BaseSymbol FROM
# 					(SELECT Symbol, SUM(GroupMarkup)AS YesterdayMarkupRebate,SUM(ClosedVolume) AS YesterdayVolume,SUM(ClosedProfit_usd) AS YesterdayProfit_usd
# 					FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE = DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 DAY AND`Group` LIKE "Real%"
# 					AND `Group` NOT LIKE "Real_Futures%" AND `GROUP` NOT LIKE "%\\\\B\_%"
# 					GROUP BY Symbol
# 					)yestTable
# 					LEFT JOIN
# 					( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)yt2
# 					ON yestTable.Symbol = yt2.Symbol
#
# 			)z2
# 			GROUP BY BaseSymbol
# 		)YesterdayClosed
# 		ON F.BaseSymbol = YesterdayClosed.BaseSymbol
#
# 		LEFT JOIN
# 		(#getting average Float
# 			SELECT basesymbol,ROUND(AVG(FloatVolume),2) AverageFloatVolume from(
# 				SELECT date,ft.Symbol,BaseSymbol,FloatVolume from(
# 					SELECT date,symbol,FloatVolume FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 MONTH
# 					AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
# 				)ft
# 				LEFT JOIN
# 				( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)ft2
# 				ON ft.Symbol = ft2.Symbol
#
# 				UNION ALL
#
# 				SELECT date,ft.Symbol,BaseSymbol,FloatVolume from(
# 					SELECT date,symbol,FloatVolume FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL 7 HOUR)- INTERVAL 1 MONTH
# 					AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
# 				)ft
# 				LEFT JOIN
# 				( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)ft2
# 				ON ft.Symbol = ft2.Symbol
# 			)Z
# 			GROUP BY basesymbol
# 		)AverageVolumet
# 		ON AverageVolumet.BaseSymbol = F.BaseSymbol
# 	""".format(time_diff_mt5=time_diff_mt5)

    sql_query = """SELECT FinalTable.Country,FinalTable.BaseSymbol,-NetVolume AS NetVolume,FloatVolume,AverageFloatVolume AS DailyAverage, -FloatProfit AS FloatProfit, TodayVolume AS TodayClosedVolume,-TodayProfitUsd AS TodayClosedProfitUsd,YesterdayVolume AS YesterdayClosedVolume,
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
                                WHEN LEFT(t3.SYMBOL,3)="USD" THEN (Profit / ((AskLast+BidLast)/2))
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
                                    Time>= DATE(NOW() - INTERVAL 5 HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
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
                                        Time>= DATE(NOW() - INTERVAL 5 HOUR) AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
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
                    SELECT date,`Group`,symbol,FloatVolume FROM mt5.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL 5 HOUR)- INTERVAL 1 MONTH 
                    AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                )ft
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5.yudi_BaseSymbol)ft2
                ON ft.Symbol = ft2.Symbol
                
                UNION ALL
                
                SELECT date,`Group`,ft.Symbol,BaseSymbol,FloatVolume from(
                    SELECT date,`Group`,symbol,FloatVolume FROM mt5_uk.yudi_daily_pnl_by_group_login_symbol WHERE DATE >= DATE(NOW() - INTERVAL 5 HOUR)- INTERVAL 1 MONTH 
                    AND `Group` LIKE "Real%"  AND `GROUP` NOT LIKE "%\\\\B\_%" AND `Group` NOT LIKE "Real_Futures%"
                )ft
                LEFT JOIN 
                ( SELECT Symbol, BaseSymbol FROM mt5_uk.yudi_BaseSymbol)ft2
                ON ft.Symbol = ft2.Symbol
            )Z
            GROUP BY Country,basesymbol
        )AverageVolumet
        ON AverageVolumet.BaseSymbol = FinalTable.BaseSymbol AND AverageVolumet.Country = FinalTable.Country"""


    print(sql_query)
    print()
    print()
    print()
    print()
    return sql_query