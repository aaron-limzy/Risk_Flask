

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
		GROUP BY Country,BaseSymbol
)FinalTable