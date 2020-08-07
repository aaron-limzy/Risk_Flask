<html><head>
<title>Symbol_Details</title>
<style type="text/css">
		.table{
			width: 100%;
			border-collapse:collapse; 
			border-spacing:0; 
		}
		.fixedThead{
			display: block;
			width: 100%;
		}
		.scrollTbody{
			display: block;
			height: 1000px;
			overflow: auto;
			width: 100%;
		}
		.table td,.table th {
			width: 140px;
			border-bottom: none;
			border-left: none;
			border-right: 1px solid #CCC;
			border-top: 1px solid #DDD;
			padding: 2px 3px 3px 4px
			
		}
		.table tr{
			border-left: 1px solid #EB8;
			border-bottom: 1px solid #B74;
		}
		thead.fixedThead tr th:last-child {
			color:#000;
			width: 190px;
		}
	</style>
</head><body>
<?php
$Symbol=$_REQUEST["Symbol"];
?>

</body></html>
<?php
$con = mysql_connect("192.168.64.73","mt4","1qaz2wsx");
if (!$con)
  {
  die('Could not connect: ' . mysql_error());
  }

mysql_select_db("live1", $con);
$sql = "SELECT `SERVER`,TICKET,LOGIN,TYPE,SYMBOL, VOLUME,OPEN_TIME,OPEN_PRICE,CLOSE_TIME,CLOSE_PRICE,REASON,SWAPS,PROFIT,`GROUP` FROM
(
SELECT 'Live1' AS `SERVER`,TICKET AS TICKET,LOGIN AS LOGIN,`GROUP` AS `GROUP`,(CASE WHEN CMD = 0 THEN 'buy' WHEN CMD = 1 THEN 'sell' ELSE CMD END) AS TYPE, SYMBOL AS SYMBOL, VOLUME*0.01 as VOLUME, OPEN_TIME AS OPEN_TIME,OPEN_PRICE AS OPEN_PRICE,CLOSE_TIME AS CLOSE_TIME,CLOSE_PRICE AS CLOSE_PRICE,
CASE WHEN mt4_trades.REASON = 0 THEN 'Client' WHEN mt4_trades.REASON = 1 THEN 'Expert' WHEN mt4_trades.REASON = 2 THEN 'Dealer' WHEN mt4_trades.REASON = 3 THEN 'Signal' WHEN mt4_trades.REASON = 4 THEN '4' WHEN mt4_trades.REASON = 5 THEN 'Mobile' ELSE 00 END AS REASON,SWAPS AS SWAPS,PROFIT AS PROFIT
FROM live1.mt4_trades WHERE SYMBOL LIKE '%".$Symbol."%' AND CLOSE_TIME > DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d 23:00:00'),INTERVAL 1 DAY) AND CMD < 2 
AND (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE BOOK = 'B'))
UNION
SELECT 'Live2' AS `SERVER`,TICKET AS TICKET,LOGIN AS LOGIN,`GROUP` AS `GROUP`,(CASE WHEN CMD = 0 THEN 'buy' WHEN CMD = 1 THEN 'sell' ELSE CMD END) AS TYPE, SYMBOL AS SYMBOL, VOLUME*0.01 as VOLUME, OPEN_TIME AS OPEN_TIME,OPEN_PRICE AS OPEN_PRICE,CLOSE_TIME AS CLOSE_TIME,CLOSE_PRICE AS CLOSE_PRICE,
CASE WHEN mt4_trades.REASON = 0 THEN 'Client' WHEN mt4_trades.REASON = 1 THEN 'Expert' WHEN mt4_trades.REASON = 2 THEN 'Dealer' WHEN mt4_trades.REASON = 3 THEN 'Signal' WHEN mt4_trades.REASON = 4 THEN '4' WHEN mt4_trades.REASON = 5 THEN 'Mobile' ELSE 00 END AS REASON,SWAPS AS SWAPS,PROFIT AS PROFIT
FROM live2.mt4_trades WHERE SYMBOL LIKE '%".$Symbol."%' AND CLOSE_TIME > DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d 23:00:00'),INTERVAL 1 DAY) AND CMD < 2 
AND (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE BOOK = 'B'))
UNION
SELECT 'Live3' AS `SERVER`,TICKET AS TICKET,LOGIN AS LOGIN,`GROUP` AS `GROUP`,(CASE WHEN CMD = 0 THEN 'buy' WHEN CMD = 1 THEN 'sell' ELSE CMD END) AS TYPE, SYMBOL AS SYMBOL, VOLUME*0.01 as VOLUME, OPEN_TIME AS OPEN_TIME,OPEN_PRICE AS OPEN_PRICE,CLOSE_TIME AS CLOSE_TIME,CLOSE_PRICE AS CLOSE_PRICE,
CASE WHEN mt4_trades.REASON = 0 THEN 'Client' WHEN mt4_trades.REASON = 1 THEN 'Expert' WHEN mt4_trades.REASON = 2 THEN 'Dealer' WHEN mt4_trades.REASON = 3 THEN 'Signal' WHEN mt4_trades.REASON = 4 THEN '4' WHEN mt4_trades.REASON = 5 THEN 'Mobile' ELSE 00 END AS REASON,SWAPS AS SWAPS,PROFIT AS PROFIT
FROM live3.mt4_trades WHERE SYMBOL LIKE '%".$Symbol."%' AND CLOSE_TIME > DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d 23:00:00'),INTERVAL 1 DAY) AND CMD < 2 
AND (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE BOOK = 'B'))
UNION
SELECT 'Live5' AS `SERVER`,TICKET AS TICKET,LOGIN AS LOGIN,`GROUP` AS `GROUP`,(CASE WHEN CMD = 0 THEN 'buy' WHEN CMD = 1 THEN 'sell' ELSE CMD END) AS TYPE, SYMBOL AS SYMBOL, VOLUME*0.01 as VOLUME, OPEN_TIME AS OPEN_TIME,OPEN_PRICE AS OPEN_PRICE,CLOSE_TIME AS CLOSE_TIME,CLOSE_PRICE AS CLOSE_PRICE,
CASE WHEN mt4_trades.REASON = 0 THEN 'Client' WHEN mt4_trades.REASON = 1 THEN 'Expert' WHEN mt4_trades.REASON = 2 THEN 'Dealer' WHEN mt4_trades.REASON = 3 THEN 'Signal' WHEN mt4_trades.REASON = 4 THEN '4' WHEN mt4_trades.REASON = 5 THEN 'Mobile' ELSE 00 END AS REASON,SWAPS AS SWAPS,PROFIT AS PROFIT
FROM live5.mt4_trades WHERE SYMBOL LIKE '%".$Symbol."%' AND CLOSE_TIME > DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d 23:00:00'),INTERVAL 1 DAY) AND CMD < 2 
AND (mt4_trades.`GROUP` IN (SELECT `GROUP` FROM live5.group_table WHERE BOOK = 'B'))
) AS A ORDER BY PROFIT*(-1);";

$result = mysql_query($sql,$con);
  $num1=mysql_num_rows($result);

  if($num1 ==0)
  {
	echo"^^ no record ^^";
  }
  else{
//表格
echo "<table class='table' id='cfd'>";
echo "<thead  class='fixedThead'><tr>";
for($i=0;$i<mysql_num_fields($result);$i++)
{
echo "<th>" . mysql_field_name($result,$i) . "</th>";
}
echo "</tr></thead><tbody class='scrollTbody'>";

//把资料移动到第一行
mysql_data_seek($result,0); 

//取资料循环
while ($row=mysql_fetch_row($result)) 
{
echo "<tr>";
for($i=0;$i<mysql_num_fields($result);$i++)
{
echo "<td>" . $row[$i] . "</td>";
}
echo "</tr>"; 
}
echo "</tbody></table>";
}

//kill掉数据链接资源
mysql_free_result($result);
?>
<script type="text/javascript">
 window.onload=function(){
               var table = document.getElementById("cfd");//获取第一个表格  
                var child = table.getElementsByTagName("tr");//获取行的第一个单元格   

                for(var i=0;i<child.length;i++){
                var ret=findZoer(child[i]);
        }
		
		var td=obj.getElementsByTagName("td");

}	
		function findZoer(obj){
   var td=obj.getElementsByTagName("td");
   for(var j=0;j<td.length;j++) {
     if(td[j].innerHTML<0){
     td[j].style.color='red';
   
   }
   }
   return false;
}
            

</script>
