BGI_MT4_Swap_Folder = "BGI_Swaps_MT4"
BGI_MT5_Swap_Folder = "BGI_Swaps_MT5"

BGI_MT5_UK_Swap_Folder = "BGI_Swaps_MT5 - Uk"		' UK Server MT5





Python_Script_Folder = "Python_Script"

Python_Get_FXPro_Swaps = Python_Script_Folder + "\\" + "Get_Swap_fxpro.py"
Python_Get_FXdd_Swaps = Python_Script_Folder + "\\" + "Get_Swaps_fxdd.py"
Python_Get_TopFX_Swaps = Python_Script_Folder + "\\" + "Get_TopFX_Crpyto_Swaps.py"

Python_Move_csv = Python_Script_Folder + "\\" + "Move_CSV.py"
'Python_Move_csv = "Move_CSV.py"
Python_Move_xls =  Python_Script_Folder + "\\" + "Move_xls.py"
Python_CFD_Dividend = Python_Script_Folder + "\\" + "CFD_Dividend_csv.py" 



C_Upload_Swap_exe = BGI_MT4_Swap_Folder + "\Swaps_Upload_NoWait.exe"
C_Swaps_to_SQL_Risk = BGI_MT4_Swap_Folder + "\Swaps_to_SQL_Risk.exe"
C_Swaps_to_SQL_BO = BGI_MT4_Swap_Folder + "\Swaps_to_SQL_BO.exe"


C_Upload_Swap_MT5_exe = BGI_MT5_Swap_Folder + "\Upload_Swaps_MT5.exe"
C_Upload_Swap_MT5_Demo_exe = BGI_MT5_Swap_Folder + "\Upload_Swaps_MT5_DEMO.exe"
C_Upload_Swap_MT5_UK_exe = BGI_MT5_UK_Swap_Folder + "\Upload_Swaps_MT5_UK.exe"
C_Upload_Swap_MT5_UK_Demo_exe = BGI_MT5_UK_Swap_Folder + "\Upload_Swaps_MT5_UK_Demo.exe"


Email_Subject_Title = "Daily_Swap"
Email_Subject_Title_Error_Flag = 0

Email_Header = "<style>p.a {font-family:Calibri;panose-1:2 15 5 2 2 2 4 3 2 4;font-size: 14.5px;}</style><p Class='a'>"
Email_Footer = "</p>"

Function Send_Email_Gmail(Email_Data, Email_Subject)
	'msgbox "Hello"
	dim MyEmail
	Set MyEmail=CreateObject("CDO.Message")

	Const cdoBasic=0 'Do not Authenticate
	Const cdoAuth=1 'Basic Authentication

	MyEmail.Subject = Email_Subject
	MyEmail.From    = "aaron.riskbgi@gmail.com"
	MyEmail.To      = "<aaron.lim@blackwellglobal.com>"
	
	MyEmail.HTMLBody= Email_Data
	'MyEmail.CreateMHTMLBody "http://www.paulsadowski.com/wsh/"

	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/sendusing")=2
	'SMTP Server
	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/smtpserver")="smtp.gmail.com"

	'SMTP Port
	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/smtpserverport")=465

	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/smtpauthenticate") = 1

	'Your UserID on the SMTP server
	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/sendusername") = "aaron.riskbgi@gmail.com"

	'Your password on the SMTP server
	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/sendpassword") = "ReportReport"

	'Use SSL for the connection (False or True)
	MyEmail.Configuration.Fields.Item ("http://schemas.microsoft.com/cdo/configuration/smtpusessl") = True
	
	'MyEmail.AddAttachment Attachment_Filename

	MyEmail.Configuration.Fields.Update
	MyEmail.Send

	Set MyEmail=nothing
end Function


' Run the C++ exe to update the symbol Swaps
Function Run_C(C_FileName, Relative_Working_Dir)
	Check_File_Exists(C_FileName)

	Dim obj
	Dim prog
	Dim Return_Val
	set obj = CreateObject("wscript.shell")		'Using a shell object.
	
	Dim FSO
	Set FSO = CreateObject("Scripting.FileSystemObject")
	Current_Dir = FSO.GetParentFolderName(WScript.ScriptFullName)	'Current Directory. 
	
	Dim Full_Path
	Full_Path = """" + Current_Dir + "\" + C_FileName + """"
	
	obj.CurrentDirectory = Current_Dir + "\" + Relative_Working_Dir		' Need to change the current directory of the script. 
	
	Return_Val = obj.run (Full_Path,7,True)
	Run_C =  Return_Val
	
	' Return 
	' 0 - No error
	' -2 No swap File found. 
	' -3 Swap file dosn't have first worksheet, or the excel can't be opened.  
	' -4 Swap file doesn't have 2nd worksheet. 
	
end Function

' Checks if the File Exists.
' Used to check if the File to run is avaliable in the dir
Function Check_File_Exists(File_Name)

	Dim Current_Dir
	Dim FSO
	Set FSO = CreateObject("Scripting.FileSystemObject")
	Current_Dir = FSO.GetParentFolderName(WScript.ScriptFullName)	'Current Directory. 
	
	If fso.FileExists( Current_Dir & "\" & File_Name) Then
		Check_File_Exists = 0
	else
		Check_File_Exists =  -1
	End If
End Function

Function Run_Python_File(Python_File)
	Dim Python_File_Exist
	Dim Python_Return
	Python_File_Exist = Check_File_Exists(Python_File)
	If Python_File_Exist <> 0 Then
		Run_Python_File = -1
		Msgbox "Python File Not Found: " + Python_File
		Exit Function
	End If
	
	Dim Current_Dir
	Dim FSO
	Set FSO = CreateObject("Scripting.FileSystemObject")
	Current_Dir = FSO.GetParentFolderName(WScript.ScriptFullName)	'Current Directory. 
	
	'FSO.SetCurrentFolder("C:\\")
	Set wshShell = CreateObject("WScript.Shell")
	wshShell.CurrentDirectory = Current_Dir	 ' Need to change the current directory of the script. 
	
	'msgbox Current_Dir + "\" + Python_File
	
	Python_Return = wshShell.Run( """" + Current_Dir + "\" + Python_File + """",0,True)	'Run the python code
	
	
	Run_Python_File = Python_Return	'Returns 0 when it has found the email. 
	if Python_Return <> 0 then	'No Mail Found. 
		Send_Email_Gmail Email_Header + "Hi,<br><br> Error Running File: " + Python_File + "  <br><br>Thanks,<br>Aaron" + Email_Footer, "ERROR: " + Email_Subject_Title + "Python File."
	end if
	
End Function

' Second function to run with different working directory. 
Function Run_Python_File_WD(Python_File, Working_Directory)
	Dim Python_File_Exist
	Dim Python_Return
	Python_File_Exist = Check_File_Exists(Python_File)
	If Python_File_Exist <> 0 Then
		Run_Python_File_WD = -1
		Msgbox "Python File Not Found: " + Python_File
		Exit Function
	End If
	
	Dim Current_Dir
	Dim FSO
	Set FSO = CreateObject("Scripting.FileSystemObject")
	Current_Dir = FSO.GetParentFolderName(WScript.ScriptFullName)	'Current Directory. 
	
	'FSO.SetCurrentFolder("C:\\")
	Set wshShell = CreateObject("WScript.Shell")
	'msgbox Current_Dir  + Working_Directory
	wshShell.CurrentDirectory = Current_Dir  + Working_Directory	 ' Need to change the current directory of the script. 
	
	Python_Return = wshShell.Run( """" + Current_Dir + "\" + Python_File + """",0,True)	'Run the python code
	
	
	Run_Python_File_WD = Python_Return	'Returns 0 when it has found the email. 
	if Python_Return <> 0 then	'No Mail Found. 
		Send_Email_Gmail Email_Header + "Hi,<br><br> File can't be found: " + Python_File + "  <br><br>Thanks,<br>Aaron" + Email_Footer, "ERROR (No Vantage Email): " + Email_Subject_Title
	end if
	
End Function


Function Calculate_Swaps(Index)

	Dim ObjExcel, ObjWB
	Set ObjExcel = CreateObject("excel.application")
	ObjExcel.DisplayAlerts = False

	scriptdir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
	Excel_Path = scriptdir + "\Swap_Calculator.xlsm"
	Set ObjWB = ObjExcel.Workbooks.Open(Excel_Path,0,False)
	ObjExcel.Visible = False
	if Index = 0 then
		Ret_Val = ObjExcel.Run("Main_Function")
	elseif Index = 1 then 
		Ret_Val = ObjExcel.Run("Change_Moving_Average")
	end if
	objWB.Save 
	ObjWB.Close True
	ObjExcel.Quit

	Set ObjExcel = Nothing
	Set ObjWB = Nothing
	Calculate_Swaps = Ret_Val
End Function


'Main_Function()


Function Open_Excel()
	'Swap comparison error. Open the Excel Sheet.
	Dim ObjExcel, ObjWB, ws
	Set ObjExcel = CreateObject("excel.application")
	ObjExcel.DisplayAlerts = False

	scriptdir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
	Excel_Path = scriptdir + "\Swap_Calculator.xlsm"
	Set ObjWB = ObjExcel.Workbooks.Open(Excel_Path,0,FALSE)
	objWB.Save 
	ObjExcel.Visible = True
	ObjExcel.DisplayAlerts = True
	ObjExcel.Activeworkbook.Sheets("Moving_Average_Compare").Activate 
		
end function

'Generate the HTML table for the results for the upload to MT4/MT5 Server. 
function Generate_HTMLTable_UploadResult(C_MT4Upload_Result, C_MT5Upload_Result,C_MT5_Demo_Upload_Result, C_MT5_UK_Upload_Result, C_MT5_UK_Demo_Upload_Result)

	Email_String = Email_Header	'For the styling of the email
	' Start Drawing the table
	Email_String = Email_String + "<html><body><table border='1' cellpadding='4'  bordercolor='Black'>"
	Email_String = Email_String + "<tr bgcolor='#82C2FF' ><th>Server</th><th>Result</th></tr>"
	
	'Check for MT4 Swap Upload

	if C_MT4Upload_Result <> 1 then
		Email_String = Email_String + "<tr bgcolor='#ffb3b3'><td>MT4</td><td>" 'Will appear Red should something go wrong. 
		Email_String = Email_String + "ERROR: "
		
		' The error Code from C++
		if C_MT4Upload_Result = -2 then
			Email_String = Email_String + "C++ Error. No Swap File Found."
		elseif C_MT4Upload_Result = -3 then
			Email_String = Email_String + "C++ Error. Swap excel can't be opened"
		elseif C_MT4Upload_Result = -4 then
			Email_String = Email_String + "C++ Error. Swap excel workbook error"
		else
			Email_String = Email_String + "C++ Error code "  + CStr(C_MT4Upload_Result) 
		End if
		
	else
	
		Email_String = Email_String + "<tr><td>MT4</td><td>"
		Email_String = Email_String + "BGI Swaps uploaded successfully to MT4."
	end if
	Email_String = Email_String + "</td></tr>"   
	
	
	
	'Check for MT5 Swap Upload
	
	Email_String = Email_String + MT5_HTML_Table_Row(C_MT5Upload_Result, "MT5 BGI")
	Email_String = Email_String + MT5_HTML_Table_Row(C_MT5_Demo_Upload_Result, "MT5 BGI Demo ")
	Email_String = Email_String + MT5_HTML_Table_Row(C_MT5_UK_Upload_Result, "MT5 UK")
	Email_String = Email_String + MT5_HTML_Table_Row(C_MT5_UK_Demo_Upload_Result, "MT5 UK Demo")
	
	
	
	Email_String = Email_String + "</table>"
	Email_String = Email_String + Email_Footer
	Generate_HTMLTable_UploadResult = Email_String

end function

' Use the MT5 Return to construct a row of HTML table. 
function MT5_HTML_Table_Row(C_MT5_result, mt5_name_string)

	if C_MT5_result <> 0 then
	
		Email_String =  "<tr bgcolor='#ffb3b3'><td>" + mt5_name_string + "</td><td>"	'Will appear Red should something go wrong. 
		Email_String = Email_String + "ERROR: "

		' If there are errors while uploading to 
		if C_MT5_result = -1 then
			Email_String = Email_String + "Can't connect to MT5 "
		elseif  C_MT5_result = -2 then
			Email_String = Email_String + "Swap Value File missing.  "
		elseif  C_MT5_result = -3 then
			Email_String = Email_String + "Can't read the Ignore_Symbol_MT5.xls File.   "
		elseif  C_MT5_result = -4 then
			Email_String = Email_String + "'Ignore_Symbol' Sheet Missing from Ignore_Symbol_MT5.xls "
		elseif  C_MT5_result = -5 then
			Email_String = Email_String + "'Ignore_Path' Sheet Missing from Ignore_Symbol_MT5.xls"
		elseif  C_MT5_result = -6 then
			Email_String = Email_String + "Swap Value File Can't be opened.   "
		else
			Email_String = Email_String + " Error code "  + CStr(C_MT5_result) + "<br>"
		end if
	else
		Email_String =  "<tr><td>" + mt5_name_string + "</td><td>"
		Email_String = Email_String + "BGI Swaps uploaded successfully to MT5. "
	end if
	Email_String = Email_String + "</td></tr>"

	MT5_HTML_Table_Row = Email_String

end function


function Draw_SQL_Table(C_SQL_Risk_Result, C_SQL_BO_Result)

	' Start Drawing the table
	Email_String = Email_String + "<html><body><table border='1' cellpadding='4'  bordercolor='Black'>"
	Email_String = Email_String + "<tr bgcolor='#82C2FF' ><th>SQL DB</th><th>Result</th></tr>"
	
	'Check for Risk SQL Upload
	Email_String = Email_String + "<tr><td>Risk</td><td>"
	if C_SQL_Risk_Result <> 0 then
		Email_String = Email_String + "ERROR: Swaps not uploaded to Risk SQL."
		Email_Title = "ERROR: SQL Swaps Failed."
		msgbox "ERROR: Swaps not uploaded to Risk SQL."
	else
		Email_String = Email_String + "BGI Swaps uploaded successfully to Risk SQL."
	end if
	Email_String = Email_String + "</td></tr>"   
	
	
	
	'Check for BO SQL Upload
	Email_String = Email_String + "<tr><td>Back Office</td><td>"
	if C_SQL_BO_Result <> 0 then
		Email_String = Email_String + "ERROR: Swaps not uploaded to BO SQL."
		'Email_Title = "ERROR: SQL Swaps Failed."
		msgbox "ERROR: Swaps not uploaded to BO SQL."
	else
		Email_String = Email_String + "BGI Swaps uploaded successfully to BO SQL. "
	end if
	Email_String = Email_String + "</td></tr>"
	Email_String = Email_String + "</table>"
	
	Draw_SQL_Table = Email_String	' Return the Email String

end Function



Function Main_Function()
	
	 
	' Get the other Swaps from other brokerages. 
	'FXPro_Swap_Flag = Run_Python_File_WD(Python_Get_FXPro_Swaps,"") 'No need FXpro anymore since they don't upload anymore. 
	FXdd_Swap_Flag = Run_Python_File_WD(Python_Get_FXdd_Swaps,"")
	Get_CSV_Flag = Run_Python_File_WD(Python_CFD_Dividend,"")	' Get the Bloomberg Dividend from Risk SQL
	
	
	If FXdd_Swap_Flag <> 0 then
		msgbox "Error in retrieving Swap files from other brokers. "
		exit function	' Want to be able to exit the function if there is a swap we cannot retrieve. 
	end if
	
	Upload_Swaps_Val = Calculate_Swaps(0)	'Calculate the BGI Swaps. Run Main Function
	
	if Upload_Swaps_Val > 0 then
		'Open the excel for viewing. 
		'Swap comparison error. Open the Excel Sheet.
		Dim ObjExcel, ObjWB, ws
		Set ObjExcel = CreateObject("excel.application")
		ObjExcel.DisplayAlerts = False

		scriptdir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
		Excel_Path = scriptdir + "\Swap_Calculator.xlsm"
		Set ObjWB = ObjExcel.Workbooks.Open(Excel_Path,0,FALSE)
		objWB.Save 
		ObjExcel.Visible = True
		ObjExcel.DisplayAlerts = True
		ObjExcel.Activeworkbook.Sheets("Moving_Average_Compare").Activate 
		Return_Value = msgbox ("Swaps comparison Error. Continue?", 4 ,"Swaps")	
		
		if Return_Value = 6 then	'If continue, Will Send out Mail. 
		
			ObjWB.Close True
			ObjExcel.Quit

			Set ObjExcel = Nothing
			Set ObjWB = Nothing
			'msgbox Return_Value
		else
			exit function
		end if
	elseif (Upload_Swaps_Val < 0) then		'Swap File not Found. 
		exit function
	end if

	Calculate_Swaps(1)	'Calculate the Moving Average
	C_MT4Upload_Result = Run_C(C_Upload_Swap_exe, BGI_MT4_Swap_Folder)		' Run upload for MT4. 
	C_MT5Upload_Result = Run_C(C_Upload_Swap_MT5_exe, BGI_MT5_Swap_Folder)	' Run the upload for MT5
	C_MT5_Demo_Upload_Result = Run_C(C_Upload_Swap_MT5_Demo_exe, BGI_MT5_Swap_Folder)	' Run the upload for MT5 Demo Server
	C_MT5_UK_Upload_Result = Run_C(C_Upload_Swap_MT5_UK_exe, BGI_MT5_UK_Swap_Folder)	' Run the upload for MT5 UK Server
	C_MT5_UK_Demo_Upload_Result = Run_C(C_Upload_Swap_MT5_UK_Demo_exe, BGI_MT5_UK_Swap_Folder)	' Run the upload for MT5 UK Server

	
	C_SQL_Risk_Result = Run_C(C_Swaps_to_SQL_Risk, BGI_MT4_Swap_Folder)		' Upload to Risk SQL
	C_SQL_BO_Result = Run_C(C_Swaps_to_SQL_BO, BGI_MT4_Swap_Folder)			' Upload to BO SQL
	

	Server_Upload_HTML_Table = Generate_HTMLTable_UploadResult(C_MT4Upload_Result, C_MT5Upload_Result,C_MT5_Demo_Upload_Result, C_MT5_UK_Upload_Result, C_MT5_UK_Demo_Upload_Result)	' Get the HTML table for the Server upload Table. 
	SQL_Upload_HTML_Table = Draw_SQL_Table(C_SQL_Risk_Result, C_SQL_BO_Result)							' Get the HTML table for the SQL upload. 

	Email_String = Email_Header + "Hi,<br><br>Results for uploading of swaps.<br>" + Server_Upload_HTML_Table + "<br>" + SQL_Upload_HTML_Table
	Email_String = Email_String + "<br><br>Thanks,<br>Aaron" + Email_Footer
	
	
	' Want to end the function so that the python code would not send the swap email out.
	If C_MT4Upload_Result < 0 or C_MT5Upload_Result <> 0 or C_SQL_Risk_Result <> 0 or C_SQL_BO_Result <> 0 or C_MT5_UK_Upload_Result <> 0 then
		Email_Subject_Title = "ERROR: " + Email_Subject_Title	'Want to append 'ERROR' to the email title. 
		
		' Message out for the errors. 
		if C_MT4Upload_Result < 0 then
			msgbox "MT4 Upload Error. "
		end if
		
		if C_MT5Upload_Result <> 0 then
			msgbox "MT5 Upload Error. "
		end if
		
		if C_SQL_Risk_Result <> 0 then
			msgbox "Risk SQL Upload Error."
		end if
		
		if C_SQL_BO_Result <> 0 then
			msgbox "BO SQL Upload Error."
		end if
		
	End if
	
	Send_Email_Gmail Email_String,  Email_Subject_Title
	
	'Clear up all the CSV and Excel File. 
	call Run_Python_File_WD(Python_Move_xls, "\\" + BGI_MT4_Swap_Folder)	'Need to run this from BGI MT4 Swap Folder.
	call Run_Python_File_WD(Python_Move_xls, "\\" + BGI_MT5_Swap_Folder)	'Need to run this from BGI MT5 Swap Folder.
	call Run_Python_File_WD(Python_Move_xls, "\\" + BGI_MT5_UK_Swap_Folder)	'Need to run this from BGI MT5 Swap Folder.
	call Run_Python_File_WD(Python_Move_csv,"")	'Move all the CSV file to a keeping folder. 
	
end Function

Main_Function()


