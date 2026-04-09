Option Explicit

' ==================== CONFIGURATION CONSTANTS ====================
Private Const CONFIG_SHEET_NAME As String = "Config"
Private Const LOG_SHEET_NAME As String = "Process Log"
Private Const SHARED_VIEW_SHEET_NAME As String = "Shared View-Batches - VP"
Private Const MAX_LOG_ROWS As Long = 100000
Private Const DEBUG_MODE As Boolean = True ' Toggle debug messages

' ==================== TYPE DEFINITIONS ====================
Private Type ProcessStats
    startTime As Date
    EndTime As Date
    TotalBatches As Long
    SuccessCount As Long
    ErrorCount As Long
End Type

Private Type PrintOrientationInfo
    orientation As Long
    fitToWidth As Long
    fitToHeight As Long
End Type

' ==================== DEBUG HELPER ====================
Private Sub DebugLog(msg As String)
    If DEBUG_MODE Then
        Debug.Print Format(Now, "hh:mm:ss") & " | " & msg
    End If
End Sub

Sub batchUpdate()
    Dim wsBatches As Worksheet
    Dim wsShared As Worksheet
    Dim lastRow As Long
    Dim sourceRange As Range
    
    On Error GoTo ErrorHandler
    
    DebugLog "batchUpdate: Starting batch update process"
    
    ' Set references to worksheets
    Set wsBatches = ThisWorkbook.Worksheets("Batches")
    Set wsShared = ThisWorkbook.Worksheets("Shared View-Batches - VP")
    
    DebugLog "batchUpdate: Clearing columns A, B, C in Batches sheet"
    ' Clear column A contents in Batches sheet
    wsBatches.Columns("A").ClearContents
    wsBatches.Columns("B").ClearContents
    wsBatches.Columns("C").ClearContents
    
    ' Find last used row in column J on Shared View-Batches - VP sheet
    lastRow = wsShared.Cells(wsShared.Rows.Count, "J").End(xlUp).row
    DebugLog "batchUpdate: Last row in column J: " & lastRow
    
    ' Define the source range from J2 down to last used cell in J
    If lastRow >= 2 Then
        Set sourceRange = wsShared.Range("J2:J" & lastRow)
        DebugLog "batchUpdate: Source range set to " & sourceRange.Address
        ' Copy values from sourceRange to A1 on Batches sheet
        wsBatches.Range("A1").Resize(sourceRange.Rows.Count, sourceRange.Columns.Count).Value = sourceRange.Value
        DebugLog "batchUpdate: Copied " & sourceRange.Rows.Count & " rows to Batches sheet"
    Else
        ' No data to copy if lastRow is less than 2
        DebugLog "batchUpdate: No data found in column J starting at row 2"
        MsgBox "No data found in column J starting at row 2 to copy.", vbInformation
    End If
    
    DebugLog "batchUpdate: Batch update completed successfully"
    Exit Sub
    
ErrorHandler:
    DebugLog "batchUpdate ERROR: " & Err.Description & " (Error " & Err.Number & ")"
    MsgBox "Error in batchUpdate: " & Err.Description, vbExclamation
End Sub

Private Function RefreshAndCalculate() As Boolean
    Dim conn As WorkbookConnection
    Dim qt As QueryTable
    Dim lo As ListObject
    Dim ws As Worksheet
    Dim connCount As Long
    Dim oledbConn As OLEDBConnection
    Dim odataConn As WorkbookConnection
    
    On Error GoTo ErrorHandler
    
    DebugLog "RefreshAndCalculate: Starting refresh process"
    
    Application.ScreenUpdating = False
    
    ' Make ALL data connections synchronous (not background)
    connCount = 0
    For Each conn In ThisWorkbook.Connections
        DebugLog "RefreshAndCalculate: Processing connection '" & conn.Name & "' Type: " & conn.Type
        
        ' Handle different connection types
        Select Case conn.Type
            Case xlConnectionTypeOLEDB
                Set oledbConn = conn.OLEDBConnection
                oledbConn.BackgroundQuery = False
                DebugLog "RefreshAndCalculate: Set OLEDB connection '" & conn.Name & "' to synchronous"
                
            Case xlConnectionTypeODBC
                conn.ODBCConnection.BackgroundQuery = False
                DebugLog "RefreshAndCalculate: Set ODBC connection '" & conn.Name & "' to synchronous"
                
            Case xlConnectionTypeMODEL, xlConnectionTypeDATAFEED, xlConnectionTypeNOSOURCE
                ' These connection types don't support BackgroundQuery
                DebugLog "RefreshAndCalculate: Skipping connection '" & conn.Name & "' - type doesn't support BackgroundQuery"
                
            Case Else
                ' Try to handle other types generically
                On Error Resume Next
                If conn.Type = xlConnectionTypeWORKSHEET Then
                    ' Worksheet connections don't have BackgroundQuery
                    DebugLog "RefreshAndCalculate: Skipping worksheet connection '" & conn.Name & "'"
                Else
                    ' Unknown type, log it
                    DebugLog "RefreshAndCalculate: Unknown connection type " & conn.Type & " for '" & conn.Name & "'"
                End If
                On Error GoTo ErrorHandler
        End Select
        
        connCount = connCount + 1
    Next conn
    DebugLog "RefreshAndCalculate: Processed " & connCount & " connections"
    
    ' Also check for QueryTables in worksheets
    For Each ws In ThisWorkbook.Worksheets
        DebugLog "RefreshAndCalculate: Checking worksheet '" & ws.Name & "' for QueryTables"
        
        ' Handle QueryTables
        On Error Resume Next
        For Each qt In ws.QueryTables
            qt.BackgroundQuery = False
            If Err.Number = 0 Then
                DebugLog "RefreshAndCalculate: Set QueryTable in '" & ws.Name & "' to synchronous"
            Else
                DebugLog "RefreshAndCalculate: Could not set QueryTable in '" & ws.Name & "' - " & Err.Description
                Err.Clear
            End If
        Next qt
        On Error GoTo ErrorHandler
        
        ' Check ListObjects (Tables) with queries
        For Each lo In ws.ListObjects
            On Error Resume Next
            If Not lo.QueryTable Is Nothing Then
                lo.QueryTable.BackgroundQuery = False
                If Err.Number = 0 Then
                    DebugLog "RefreshAndCalculate: Set ListObject '" & lo.Name & "' in '" & ws.Name & "' to synchronous"
                Else
                    DebugLog "RefreshAndCalculate: Could not set ListObject '" & lo.Name & "' - " & Err.Description
                    Err.Clear
                End If
            End If
            On Error GoTo ErrorHandler
        Next lo
    Next ws
    
    ' Now RefreshAll will wait for completion
    DebugLog "RefreshAndCalculate: Executing RefreshAll"
    On Error Resume Next
    ThisWorkbook.RefreshAll
    If Err.Number <> 0 Then
        DebugLog "RefreshAndCalculate: RefreshAll warning - " & Err.Description
        Err.Clear
        
        ' Try alternative refresh approach
        DebugLog "RefreshAndCalculate: Trying individual connection refresh"
        For Each conn In ThisWorkbook.Connections
            conn.Refresh
            DebugLog "RefreshAndCalculate: Refreshed connection '" & conn.Name & "'"
        Next conn
    End If
    On Error GoTo ErrorHandler
    
    ' Re-enable automatic calculation
    DebugLog "RefreshAndCalculate: Enabling automatic calculation"
    Application.Calculation = xlCalculationAutomatic
    Application.CalculateFull
    
    ' Save the workbook
    DebugLog "RefreshAndCalculate: Saving workbook"
    ThisWorkbook.Save
    
    Application.ScreenUpdating = True
    DebugLog "RefreshAndCalculate: Refresh completed successfully"
    RefreshAndCalculate = True
    Exit Function
    
ErrorHandler:
    DebugLog "RefreshAndCalculate ERROR: " & Err.Description & " (Error " & Err.Number & ")"
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    RefreshAndCalculate = False
End Function

' ==================== MAIN PROCEDURES ====================
Sub ProcessAllBatches()
    Dim stats As ProcessStats
    Dim errorMessage As String
    
    On Error GoTo ErrorHandler
    
    DebugLog "========================= STARTING NEW BATCH PROCESS ========================="
    DebugLog "ProcessAllBatches: Starting at " & Now
    
    stats.startTime = Now
    Call OptimizePerformance(True)

    Call LogMessage("INFO", "Refreshing all data connections...")
    If RefreshAndCalculate() Then
        Call LogMessage("INFO", "Data refresh completed successfully")
    Else
        Call LogMessage("ERROR", "Data refresh failed")
        DebugLog "ProcessAllBatches: Data refresh failed, exiting"
        Exit Sub
    End If

    DebugLog "ProcessAllBatches: Ensuring required sheets exist"
    Call EnsureLogSheetExists
    Call EnsureSharedViewSheetExists
    Call LogMessage("INFO", "Batch processing started")
    
    DebugLog "ProcessAllBatches: Calling batchUpdate"
    Call batchUpdate
    
    DebugLog "ProcessAllBatches: Starting core batch processing"
    If ProcessBatchesCore(stats, errorMessage) Then
        stats.EndTime = Now
        DebugLog "ProcessAllBatches: Core processing completed successfully"
        DebugLog "ProcessAllBatches: Total: " & stats.TotalBatches & ", Success: " & stats.SuccessCount & ", Errors: " & stats.ErrorCount
        Call LogMessage("INFO", "Batch processing completed successfully. " & _
                            "Processed: " & stats.TotalBatches & ", " & _
                            "Success: " & stats.SuccessCount & ", " & _
                            "Errors: " & stats.ErrorCount)
        MsgBox "Batch processing completed!" & vbCrLf & _
            "Total: " & stats.TotalBatches & vbCrLf & _
            "Success: " & stats.SuccessCount & vbCrLf & _
            "Errors: " & stats.ErrorCount, vbInformation
    Else
        DebugLog "ProcessAllBatches ERROR: " & errorMessage
        Call LogMessage("ERROR", "Batch processing failed: " & errorMessage)
        MsgBox "Batch processing failed!" & vbCrLf & errorMessage, vbExclamation
    End If
    
Cleanup:
    DebugLog "ProcessAllBatches: Cleaning up and restoring settings"
    Call OptimizePerformance(False)
    DebugLog "========================= BATCH PROCESS COMPLETED ========================="
    Exit Sub
    
ErrorHandler:
    errorMessage = "Unexpected error: " & Err.Description
    DebugLog "ProcessAllBatches CRITICAL ERROR: " & errorMessage & " (Error " & Err.Number & ")"
    Call LogMessage("ERROR", errorMessage)
    Resume Cleanup
End Sub

Private Function ProcessBatchesCore(ByRef stats As ProcessStats, ByRef errorMessage As String) As Boolean
    Dim wsBatches As Worksheet
    Dim wsPLTemplate As Worksheet
    Dim wsSCITemplate As Worksheet
    Dim lastRow As Long
    Dim currentRow As Long
    Dim batchValue As String
    Dim savePath As String
    
    On Error GoTo ErrorHandler
    
    DebugLog "ProcessBatchesCore: Starting core processing"
    
    If Not SetWorksheetReferences(wsBatches, wsPLTemplate, wsSCITemplate, errorMessage) Then
        DebugLog "ProcessBatchesCore: Failed to set worksheet references - " & errorMessage
        ProcessBatchesCore = False
        Exit Function
    End If
    
    savePath = GetConfiguredSavePath()
    DebugLog "ProcessBatchesCore: Save path configured as '" & savePath & "'"
    
    If savePath = "" Then
        errorMessage = "Save path not configured or invalid"
        DebugLog "ProcessBatchesCore: Save path is empty or invalid"
        ProcessBatchesCore = False
        Exit Function
    End If
    
    lastRow = wsBatches.Cells(wsBatches.Rows.Count, "A").End(xlUp).row
    DebugLog "ProcessBatchesCore: Found " & lastRow & " rows to process"
    
    For currentRow = 1 To lastRow
        batchValue = Trim(CStr(wsBatches.Cells(currentRow, 1).Value))
        
        If Len(batchValue) > 0 Then
            stats.TotalBatches = stats.TotalBatches + 1
            DebugLog "ProcessBatchesCore: Processing row " & currentRow & " of " & lastRow & " - Batch: " & batchValue
            
            If ProcessSingleBatch(batchValue, currentRow, wsBatches, wsPLTemplate, wsSCITemplate, savePath) Then
                stats.SuccessCount = stats.SuccessCount + 1
                DebugLog "ProcessBatchesCore: Successfully processed batch " & batchValue
            Else
                stats.ErrorCount = stats.ErrorCount + 1
                DebugLog "ProcessBatchesCore: Failed to process batch " & batchValue
            End If
        Else
            DebugLog "ProcessBatchesCore: Skipping empty row " & currentRow
        End If
    Next currentRow
    
    DebugLog "ProcessBatchesCore: All batches processed"
    ProcessBatchesCore = True
    Exit Function
    
ErrorHandler:
    errorMessage = "Error in ProcessBatchesCore: " & Err.Description
    DebugLog "ProcessBatchesCore ERROR: " & errorMessage & " (Error " & Err.Number & ")"
    ProcessBatchesCore = False
End Function

Private Function ProcessSingleBatch(batchValue As String, rowNum As Long, _
                                wsBatches As Worksheet, wsPLTemplate As Worksheet, _
                                wsSCITemplate As Worksheet, savePath As String) As Boolean
    Dim wb As Workbook
    Dim fileName As String
    Dim plCINumber As String
    Dim sciCINumber As String
    
    On Error GoTo ErrorHandler
    
    DebugLog "ProcessSingleBatch: Starting processing for batch " & batchValue
    Call LogMessage("INFO", "Processing batch: " & batchValue)
    
    DebugLog "ProcessSingleBatch: Updating templates with batch value"
    Call UpdateTemplates(batchValue, wsPLTemplate, wsSCITemplate)
    
    DebugLog "ProcessSingleBatch: Generating CI numbers"
    plCINumber = GenerateCINumber(batchValue, "PL")
    sciCINumber = GenerateCINumber(batchValue, "CI")
    
    DebugLog "ProcessSingleBatch: Generated PL CI: " & plCINumber & ", SCI CI: " & sciCINumber
    
    wsPLTemplate.Range("M110").Value = plCINumber
    wsSCITemplate.Range("M119").Value = sciCINumber
    
    DebugLog "ProcessSingleBatch: Calculating sheets"
    Application.Calculate
    DoEvents
    
    fileName = batchValue & "_RG.xlsx"
    DebugLog "ProcessSingleBatch: Creating workbook with filename: " & fileName
    
    Set wb = CreateBatchWorkbook(batchValue, wsPLTemplate, wsSCITemplate, savePath, fileName)
    
    If Not wb Is Nothing Then
        DebugLog "ProcessSingleBatch: Workbook created successfully, updating batch sheet"
        wsBatches.Cells(rowNum, 2).Value = plCINumber
        wsBatches.Cells(rowNum, 3).Value = sciCINumber
        
        DebugLog "ProcessSingleBatch: Appending to shared view"
        Call AppendToSharedView(batchValue, plCINumber, sciCINumber)
        
        Call LogMessage("INFO", "Successfully created: " & fileName & _
                            " | PL CI: " & plCINumber & " | SCI CI: " & sciCINumber)
        ProcessSingleBatch = True
    Else
        DebugLog "ProcessSingleBatch: Failed to create workbook"
        Call LogMessage("ERROR", "Failed to create workbook for batch: " & batchValue)
        ProcessSingleBatch = False
    End If
    
    Exit Function
    
ErrorHandler:
    DebugLog "ProcessSingleBatch ERROR: " & Err.Description & " (Error " & Err.Number & ") for batch " & batchValue
    Call LogMessage("ERROR", "Error processing batch " & batchValue & ": " & Err.Description)
    ProcessSingleBatch = False
End Function

' ==================== CI NUMBER GENERATION ====================
Private Function GenerateCINumber(batchNumber As String, docType As String) As String
    Dim datePrefix As String
    Dim batchSuffix As String
    Dim typeCode As String
    Dim Version As String
    
    DebugLog "GenerateCINumber: Generating CI for batch " & batchNumber & ", type " & docType
    
    datePrefix = Format(Date, "YYYYMMDD")
    
    ' Fixed: Check batch number length before extracting suffix
    If Len(batchNumber) >= 7 Then
        batchSuffix = Right(batchNumber, 7)
    Else
        batchSuffix = batchNumber
        DebugLog "GenerateCINumber: Warning - batch number shorter than 7 chars, using full: " & batchNumber
    End If
    
    Version = "V7"
    
    If docType = "PL" Then
        typeCode = "PL"
    Else
        typeCode = "CI"
    End If
    
    GenerateCINumber = datePrefix & "-" & typeCode & "-" & batchSuffix & "-" & Version
    DebugLog "GenerateCINumber: Generated CI number: " & GenerateCINumber
End Function

' ==================== SHARED VIEW MANAGEMENT ====================
Private Sub EnsureSharedViewSheetExists()
    Dim ws As Worksheet
    Dim sheetExists As Boolean
    
    DebugLog "EnsureSharedViewSheetExists: Checking for sheet '" & SHARED_VIEW_SHEET_NAME & "'"
    
    For Each ws In ThisWorkbook.Sheets
        If ws.Name = SHARED_VIEW_SHEET_NAME Then
            sheetExists = True
            DebugLog "EnsureSharedViewSheetExists: Sheet already exists"
            Exit For
        End If
    Next ws
    
    If Not sheetExists Then
        DebugLog "EnsureSharedViewSheetExists: Creating new shared view sheet"
        Set ws = ThisWorkbook.Sheets.Add(After:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count))
        ws.Name = SHARED_VIEW_SHEET_NAME
        
        With ws
            .Range("A1:F1").Value = Array("S.No", "Date", "Batch Number", "Batch Number", "PL Key", "CI Key")
            .Range("A1:F1").Font.Bold = True
            .Range("A1:F1").Interior.ColorIndex = 15
            .Columns("A:A").ColumnWidth = 10
            .Columns("B:B").ColumnWidth = 12
            .Columns("C:D").ColumnWidth = 15
            .Columns("E:F").ColumnWidth = 20
        End With
        DebugLog "EnsureSharedViewSheetExists: Sheet created with headers"
    End If
End Sub

Private Sub AppendToSharedView(batchNumber As String, plKey As String, ciKey As String)
    Dim wsShared As Worksheet
    Dim nextRow As Long
    Dim serialNumber As Long
    
    On Error Resume Next
    Set wsShared = ThisWorkbook.Sheets(SHARED_VIEW_SHEET_NAME)
    On Error GoTo 0
    
    If wsShared Is Nothing Then
        DebugLog "AppendToSharedView: WARNING - Shared view sheet not found"
        Exit Sub
    End If
    
    nextRow = wsShared.Cells(wsShared.Rows.Count, "A").End(xlUp).row + 1
    DebugLog "AppendToSharedView: Appending to row " & nextRow
    
    If nextRow = 2 Then
        serialNumber = 1
    Else
        ' Fixed: Add validation for serial number
        On Error Resume Next
        serialNumber = Val(wsShared.Cells(nextRow - 1, 1).Value) + 1
        If Err.Number <> 0 Then
            serialNumber = nextRow - 1
            DebugLog "AppendToSharedView: Warning - Could not parse previous serial number, using row-based numbering"
        End If
        On Error GoTo 0
    End If
    
    With wsShared
        .Cells(nextRow, 1).Value = serialNumber
        .Cells(nextRow, 2).Value = Date
        .Cells(nextRow, 3).Value = batchNumber
        .Cells(nextRow, 4).Value = batchNumber
        .Cells(nextRow, 5).Value = plKey
        .Cells(nextRow, 6).Value = ciKey
    End With
    
    DebugLog "AppendToSharedView: Added entry #" & serialNumber & " for batch " & batchNumber
End Sub

' ==================== TEMPLATE HANDLING ====================
Private Sub UpdateTemplates(batchValue As String, wsPL As Worksheet, wsSCI As Worksheet)
    DebugLog "UpdateTemplates: Clearing template cells"
    
    wsPL.Range("M10:O10").ClearContents
    wsSCI.Range("K8:M8").ClearContents
    
    wsPL.Range("M110").ClearContents
    wsSCI.Range("M119").ClearContents
    
    DebugLog "UpdateTemplates: Setting batch value " & batchValue & " in templates"
    wsPL.Range("M10").Value = batchValue
    wsSCI.Range("K9").Value = batchValue  ' Note: This was K9, not K8
    
    DebugLog "UpdateTemplates: Template update complete"
End Sub

Private Function CreateBatchWorkbook(batchName As String, wsPLTemplate As Worksheet, _
                                wsSCITemplate As Worksheet, savePath As String, _
                                fileName As String) As Workbook
    Dim wb As Workbook
    Dim wsPL As Worksheet, wsCI As Worksheet
    Dim fullPath As String
    
    On Error GoTo ErrorHandler
    
    DebugLog "CreateBatchWorkbook: Creating new workbook for batch " & batchName
    
    Application.DisplayAlerts = False
    
    Set wb = Workbooks.Add(xlWBATWorksheet)
    wb.Sheets(1).Name = "PL"
    wb.Sheets.Add(After:=wb.Sheets(1)).Name = "CI"
    
    DebugLog "CreateBatchWorkbook: Created workbook with PL and CI sheets"
    
    Set wsPL = wb.Sheets("PL")
    Set wsCI = wb.Sheets("CI")
    
    DebugLog "CreateBatchWorkbook: Processing PL sheet"
    Call ProcessPLSheet(wsPLTemplate, wsPL)
    
    DebugLog "CreateBatchWorkbook: Processing CI sheet"
    Call ProcessCISheet(wsSCITemplate, wsCI)
    
    ' Apply consistent print settings to both sheets
    DebugLog "CreateBatchWorkbook: Applying print settings"
    Call ApplyConsistentPrintSettings(wsPL, wsCI)
    
    fullPath = savePath & Application.PathSeparator & fileName
    DebugLog "CreateBatchWorkbook: Saving workbook to " & fullPath
    
    wb.SaveAs fullPath, FileFormat:=xlOpenXMLWorkbook
    wb.Close SaveChanges:=False
    
    DebugLog "CreateBatchWorkbook: Workbook saved and closed successfully"
    Set CreateBatchWorkbook = wb
    Application.DisplayAlerts = True
    Exit Function
    
ErrorHandler:
    DebugLog "CreateBatchWorkbook ERROR: " & Err.Description & " (Error " & Err.Number & ")"
    Application.DisplayAlerts = True
    If Not wb Is Nothing Then
        wb.Close SaveChanges:=False
    End If
    Set CreateBatchWorkbook = Nothing
End Function

Private Sub ProcessPLSheet(sourceSheet As Worksheet, targetSheet As Worksheet)
    Dim lastRow As Long
    Dim lastCol As Long
    Dim i As Long
    Dim sourceRowHeights() As Double

    DebugLog "ProcessPLSheet: Starting PL sheet processing"
    
    lastRow = FindLastRow(sourceSheet)
    lastCol = FindLastColumn(sourceSheet)
    
    DebugLog "ProcessPLSheet: Source dimensions - Rows: " & lastRow & ", Columns: " & lastCol

    ReDim sourceRowHeights(1 To lastRow)
    For i = 1 To lastRow
        sourceRowHeights(i) = sourceSheet.Rows(i).RowHeight
    Next i

    DebugLog "ProcessPLSheet: Copying values"
    With sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol))
        targetSheet.Range("A1").Resize(.Rows.Count, .Columns.Count).Value = .Value
    End With

    DebugLog "ProcessPLSheet: Applying row heights"
    For i = 1 To lastRow
        targetSheet.Rows(i).RowHeight = sourceRowHeights(i)
    Next i

    DebugLog "ProcessPLSheet: Copying formats"
    sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol)).Copy
    targetSheet.Range("A1").PasteSpecial xlPasteFormats
    targetSheet.Range("A1").PasteSpecial xlPasteColumnWidths

    DebugLog "ProcessPLSheet: Copying formulas for specific ranges"
    CopyFormulas sourceSheet.Range("I99:O99"), targetSheet.Range("I99:O99")
    CopyFormulas sourceSheet.Range("D108:D111"), targetSheet.Range("D108:D111")

    Application.Calculate
    targetSheet.Calculate
    DoEvents
    
    DebugLog "ProcessPLSheet: Adjusting product names"
    Call AdjustProductNames(targetSheet.Range("E23:E98"))
    
    ' AutoFit rows 23:98 BEFORE deleting blank rows
    targetSheet.Range("23:98").EntireRow.AutoFit
    
    DebugLog "ProcessPLSheet: Deleting blank rows"
    Call DeleteBlankRowsInRange(targetSheet.Range("A23:A98"))

    ' AutoFit columns for the entire data range
    DebugLog "ProcessPLSheet: Auto-fitting columns"
    With targetSheet.Range(targetSheet.Cells(1, 1), targetSheet.Cells(lastRow, lastCol))
        .EntireColumn.AutoFit
    End With
    
    targetSheet.Columns("J:L").ColumnWidth = 10

    Application.CutCopyMode = False
    DebugLog "ProcessPLSheet: PL sheet processing complete"
End Sub

Private Sub ProcessCISheet(sourceSheet As Worksheet, targetSheet As Worksheet)
    Dim lastRow As Long
    Dim lastCol As Long
    Dim formulaRanges As Variant
    Dim i As Long
    Dim sourceRowHeights() As Double

    DebugLog "ProcessCISheet: Starting CI sheet processing"
    
    lastRow = FindLastRow(sourceSheet)
    lastCol = FindLastColumn(sourceSheet)
    
    DebugLog "ProcessCISheet: Source dimensions - Rows: " & lastRow & ", Columns: " & lastCol

    ReDim sourceRowHeights(1 To lastRow)
    For i = 1 To lastRow
        sourceRowHeights(i) = sourceSheet.Rows(i).RowHeight
    Next i

    DebugLog "ProcessCISheet: Copying values"
    With sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol))
        targetSheet.Range("A1").Resize(.Rows.Count, .Columns.Count).Value = .Value
    End With

    DebugLog "ProcessCISheet: Applying row heights"
    For i = 1 To lastRow
        targetSheet.Rows(i).RowHeight = sourceRowHeights(i)
    Next i

    DebugLog "ProcessCISheet: Copying formats"
    sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol)).Copy
    targetSheet.Range("A1").PasteSpecial xlPasteFormats
    targetSheet.Range("A1").PasteSpecial xlPasteColumnWidths

    DebugLog "ProcessCISheet: Copying merged cells"
    CopyMergedCells sourceSheet, targetSheet, lastRow, lastCol

    DebugLog "ProcessCISheet: Copying formulas"
    CopyFormulas sourceSheet.Range("H99:M106"), targetSheet.Range("H99:M106")

    formulaRanges = Array("B117", "B118", "B123", "B124")
    For i = 0 To UBound(formulaRanges)
        CopyFormulas sourceSheet.Range(formulaRanges(i)), targetSheet.Range(formulaRanges(i))
    Next i

    Application.Calculate
    targetSheet.Calculate
    DoEvents

    DebugLog "ProcessCISheet: Adjusting product names"
    Call AdjustProductNames(targetSheet.Range("E23:E98"))
    targetSheet.Range("23:98").EntireRow.AutoFit
    
    DebugLog "ProcessCISheet: Deleting blank rows"
    Call DeleteBlankRowsInRange(targetSheet.Range("A23:A98"))

    ' AutoFit columns for the entire data range
    DebugLog "ProcessCISheet: Auto-fitting columns"
    With targetSheet.Range(targetSheet.Cells(1, 1), targetSheet.Cells(lastRow, lastCol))
        .EntireColumn.AutoFit
    End With
    
    targetSheet.Columns("H:I").ColumnWidth = 10
    targetSheet.Columns("M:M").ColumnWidth = 25
    
    Application.CutCopyMode = False
    DebugLog "ProcessCISheet: CI sheet processing complete"
End Sub

' ==================== IMPROVED PRINT SETTINGS ====================
Private Sub ApplyConsistentPrintSettings(wsPL As Worksheet, wsCI As Worksheet)
    On Error Resume Next
    
    Dim printInfo As PrintOrientationInfo
    
    DebugLog "ApplyConsistentPrintSettings: Determining best print orientation"
    
    ' Determine the best orientation for both sheets
    printInfo = DetermineBestOrientation(wsPL, wsCI)
    
    DebugLog "ApplyConsistentPrintSettings: Selected orientation: " & _
             IIf(printInfo.orientation = xlPortrait, "Portrait", "Landscape") & _
             ", FitToWidth: " & printInfo.fitToWidth & ", FitToHeight: " & printInfo.fitToHeight
    
    ' Apply the same print settings to both sheets
    Call ApplyPrintSettingsToSheet(wsPL, printInfo)
    Call ApplyPrintSettingsToSheet(wsCI, printInfo)
    
    DebugLog "ApplyConsistentPrintSettings: Print settings applied to both sheets"
    On Error GoTo 0
End Sub

Private Function DetermineBestOrientation(wsPL As Worksheet, wsCI As Worksheet) As PrintOrientationInfo
    Dim plUsedRange As Range
    Dim ciUsedRange As Range
    Dim plWidth As Double, plHeight As Double
    Dim ciWidth As Double, ciHeight As Double
    Dim maxWidth As Double, maxHeight As Double
    Dim pageWidthPortrait As Double, pageHeightPortrait As Double
    Dim pageWidthLandscape As Double, pageHeightLandscape As Double
    Dim portraitVerticalUsage As Double, landscapeVerticalUsage As Double
    Dim result As PrintOrientationInfo
    
    DebugLog "DetermineBestOrientation: Analyzing sheet dimensions"
    
    ' Get used ranges
    Set plUsedRange = wsPL.usedRange
    Set ciUsedRange = wsCI.usedRange
    
    ' Calculate content dimensions for both sheets
    plWidth = GetRangeWidth(plUsedRange)
    plHeight = GetRangeHeight(plUsedRange)
    ciWidth = GetRangeWidth(ciUsedRange)
    ciHeight = GetRangeHeight(ciUsedRange)
    
    DebugLog "DetermineBestOrientation: PL dimensions - Width: " & plWidth & ", Height: " & plHeight
    DebugLog "DetermineBestOrientation: CI dimensions - Width: " & ciWidth & ", Height: " & ciHeight
    
    ' Fixed: Use maximum dimensions from both sheets
    maxWidth = Application.Max(plWidth, ciWidth)
    maxHeight = Application.Max(plHeight, ciHeight)
    
    DebugLog "DetermineBestOrientation: Max dimensions - Width: " & maxWidth & ", Height: " & maxHeight
    
    ' A4 page dimensions in points (minus margins)
    pageWidthPortrait = Application.InchesToPoints(8.27) - Application.InchesToPoints(1)   ' 7.27 inches usable
    pageHeightPortrait = Application.InchesToPoints(11.69) - Application.InchesToPoints(1.5) ' 10.19 inches usable
    pageWidthLandscape = pageHeightPortrait
    pageHeightLandscape = pageWidthPortrait
    
    ' Calculate vertical usage for both orientations
    Dim portraitScaleFactor As Double, landscapeScaleFactor As Double
    portraitScaleFactor = pageWidthPortrait / maxWidth
    landscapeScaleFactor = pageWidthLandscape / maxWidth
    
    portraitVerticalUsage = (maxHeight * portraitScaleFactor) / pageHeightPortrait
    landscapeVerticalUsage = (maxHeight * landscapeScaleFactor) / pageHeightLandscape
    
    DebugLog "DetermineBestOrientation: Portrait vertical usage: " & Format(portraitVerticalUsage, "0.00")
    DebugLog "DetermineBestOrientation: Landscape vertical usage: " & Format(landscapeVerticalUsage, "0.00")
    
    ' Decision logic: Choose orientation that provides better fit
    ' Priority: 1) Fits on one page, 2) Better vertical space utilization
    If portraitVerticalUsage > 0.6 Then
        ' Portrait uses good amount of vertical space
        result.orientation = xlPortrait
        result.fitToWidth = 1
        DebugLog "DetermineBestOrientation: Selected Portrait mode"
    Else
        ' Both have poor utilization, choose landscape as default
        result.orientation = xlLandscape
        result.fitToHeight = 1
        DebugLog "DetermineBestOrientation: Selected Landscape mode"
    End If
    
    DetermineBestOrientation = result
End Function

Private Sub ApplyPrintSettingsToSheet(ws As Worksheet, printInfo As PrintOrientationInfo)
    On Error Resume Next

    Dim usedRange As Range
    Set usedRange = ws.usedRange
    
    DebugLog "ApplyPrintSettingsToSheet: Applying settings to " & ws.Name

    With ws.PageSetup
        .orientation = printInfo.orientation
        .PaperSize = xlPaperA4
        .Zoom = False
        .FitToPagesWide = printInfo.fitToWidth

        If printInfo.fitToHeight > 0 Then
            .FitToPagesTall = printInfo.fitToHeight
        Else
            .FitToPagesTall = False
        End If

        .LeftMargin = Application.CentimetersToPoints(1.27)
        .RightMargin = Application.CentimetersToPoints(1.27)
        .TopMargin = Application.CentimetersToPoints(1.9)
        .BottomMargin = Application.CentimetersToPoints(1.9)

        ' .CenterHeader = "&A"
        ' .RightHeader = "Page &P"

        .PrintArea = usedRange.Address

        ' Must be at the end
        .CenterHorizontally = True
        '.CenterVertically = True
    End With
    
    DebugLog "ApplyPrintSettingsToSheet: Settings applied successfully"

    On Error GoTo 0
End Sub

' ==================== UTILITY FUNCTIONS ====================
Private Sub CopyMergedCells(sourceSheet As Worksheet, targetSheet As Worksheet, maxRow As Long, maxCol As Long)
    Dim cell As Range
    Dim mergeArea As Range
    Dim mergedCount As Long
    
    On Error Resume Next
    
    DebugLog "CopyMergedCells: Starting to copy merged cells"
    mergedCount = 0
    
    For Each cell In sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(maxRow, maxCol))
        If cell.MergeCells Then
            Set mergeArea = cell.mergeArea
            If cell.Address = mergeArea.Cells(1, 1).Address Then
                targetSheet.Range(mergeArea.Address).Merge
                mergedCount = mergedCount + 1
            End If
        End If
    Next cell
    
    DebugLog "CopyMergedCells: Copied " & mergedCount & " merged cell areas"
    
    On Error GoTo 0
End Sub

Private Sub CopyFormulas(sourceRange As Range, targetRange As Range)
    On Error Resume Next
    DebugLog "CopyFormulas: Copying formulas from " & sourceRange.Address & " to " & targetRange.Address
    targetRange.Formula = sourceRange.Formula
    If Err.Number <> 0 Then
        DebugLog "CopyFormulas: Warning - Could not copy formula: " & Err.Description
    End If
    On Error GoTo 0
End Sub

Private Sub DeleteBlankRowsInRange(rng As Range)
    Dim rowsToDelete As Range
    Dim cell As Range
    Dim blankCount As Long
    
    DebugLog "DeleteBlankRowsInRange: Checking range " & rng.Address & " for blank rows"
    blankCount = 0
    
    For Each cell In rng
        If IsEmpty(cell.Value) Then
            blankCount = blankCount + 1
            If rowsToDelete Is Nothing Then
                Set rowsToDelete = cell.EntireRow
            Else
                Set rowsToDelete = Union(rowsToDelete, cell.EntireRow)
            End If
        End If
    Next cell
    
    If Not rowsToDelete Is Nothing Then
        DebugLog "DeleteBlankRowsInRange: Deleting " & blankCount & " blank rows"
        rowsToDelete.Delete
    Else
        DebugLog "DeleteBlankRowsInRange: No blank rows found"
    End If
End Sub

Private Sub AdjustProductNames(rng As Range)
    Dim cell As Range
    Dim cellText As String
    Dim maxCharsPerLine As Long
    Dim maxLines As Long
    Dim totalMaxChars As Long
    Dim originalHeight As Double
    Dim requiredLines As Long
    Dim adjustedCount As Long
    
    DebugLog "AdjustProductNames: Adjusting product names in range " & rng.Address
    
    maxLines = 3
    originalHeight = 18
    maxCharsPerLine = 32
    adjustedCount = 0
    
    For Each cell In rng
        If Not IsEmpty(cell.Value) Then
            cellText = CStr(cell.Value)
            totalMaxChars = maxCharsPerLine * maxLines

            requiredLines = Application.WorksheetFunction.RoundUp(Len(cellText) / maxCharsPerLine, 0)
            
            cell.WrapText = True
            adjustedCount = adjustedCount + 1
            
            With cell.Font
                .Name = "Arial"
                .Size = 12
            End With
            
            Select Case requiredLines
                Case 1
                    cell.RowHeight = originalHeight
                Case 2
                    cell.RowHeight = originalHeight + 15
                Case 3
                    cell.RowHeight = originalHeight + 30
                Case Is > 3
                    Dim truncateAt As Long
                    truncateAt = totalMaxChars - 3
                    
                    Dim breakPoint As Long
                    Dim lastSpace As Long, lastComma As Long, lastHyphen As Long
                    
                    lastSpace = InStrRev(Left(cellText, truncateAt), " ")
                    lastComma = InStrRev(Left(cellText, truncateAt), ",")
                    lastHyphen = InStrRev(Left(cellText, truncateAt), "-")
                    
                    breakPoint = Application.WorksheetFunction.Max(lastSpace, lastComma, lastHyphen)
                    
                    If breakPoint > truncateAt * 0.8 Then
                        cell.Value = RTrim(Left(cellText, breakPoint)) & "..."
                    Else
                        cell.Value = Left(cellText, truncateAt) & "..."
                    End If
                    
                    cell.RowHeight = originalHeight + 45
                    DebugLog "AdjustProductNames: Truncated long text in cell " & cell.Address
            End Select
            
            If requiredLines > 1 Then
                cell.RowHeight = cell.RowHeight + 2
            End If
            
            cell.VerticalAlignment = xlTop
        End If
    Next cell
    
    DebugLog "AdjustProductNames: Adjusted " & adjustedCount & " cells"
End Sub

Private Function FindLastRow(ws As Worksheet) As Long
    Dim lastRow As Long
    On Error Resume Next
    lastRow = ws.Cells.Find(What:="*", SearchOrder:=xlByRows, SearchDirection:=xlPrevious).row
    On Error GoTo 0
    If lastRow = 0 Then lastRow = 1
    If lastRow > 150 Then
        DebugLog "FindLastRow: Limiting last row from " & lastRow & " to 150"
        lastRow = 150
    End If
    FindLastRow = lastRow
End Function

Private Function FindLastColumn(ws As Worksheet) As Long
    Dim lastCol As Long
    On Error Resume Next
    lastCol = ws.Cells.Find(What:="*", SearchOrder:=xlByColumns, SearchDirection:=xlPrevious).Column
    On Error GoTo 0
    If lastCol = 0 Then lastCol = 26
    If lastCol > 26 Then
        DebugLog "FindLastColumn: Limiting last column from " & lastCol & " to 26"
        lastCol = 26
    End If
    FindLastColumn = lastCol
End Function

' ==================== CONFIGURATION FUNCTIONS ====================
Private Function GetConfiguredSavePath() As String
    Dim configSheet As Worksheet
    Dim configPath As String
    Dim defaultPath As String
    
    DebugLog "GetConfiguredSavePath: Looking for configured save path"
    
    On Error Resume Next
    Set configSheet = ThisWorkbook.Sheets(CONFIG_SHEET_NAME)
    On Error GoTo 0
    
    If Not configSheet Is Nothing Then
        configPath = Trim(CStr(configSheet.Range("B2").Value))
        DebugLog "GetConfiguredSavePath: Found config path: " & configPath
    Else
        DebugLog "GetConfiguredSavePath: Config sheet not found"
    End If
    
    #If Mac Then
        Const isMac = True
    #Else
        Const isMac = False
    #End If
        
    DebugLog "GetConfiguredSavePath: Running on " & IIf(isMac, "Mac", "Windows")
    
    If configPath = "" Or Not FolderExists(configPath) Then
        DebugLog "GetConfiguredSavePath: Config path empty or doesn't exist, using default"

        #If Mac Then
            ' Fixed: Use dynamic username on Mac
            defaultPath = "/Users/" & Environ("USER") & "/Documents/Shipment documents"
        #Else
            defaultPath = Environ("USERPROFILE") & "\Documents\Shipment documents"
        #End If
        
        DebugLog "GetConfiguredSavePath: Default path: " & defaultPath
        
        If FolderExists(defaultPath) Then
            GetConfiguredSavePath = defaultPath
        Else
            DebugLog "GetConfiguredSavePath: Default path doesn't exist, attempting to create"
            On Error Resume Next
            MkDir defaultPath
            On Error GoTo 0
            
            If FolderExists(defaultPath) Then
                GetConfiguredSavePath = defaultPath
                DebugLog "GetConfiguredSavePath: Created default path successfully"
            Else
                GetConfiguredSavePath = Application.DefaultFilePath
                DebugLog "GetConfiguredSavePath: Could not create default path, using Excel default: " & Application.DefaultFilePath
            End If
        End If
    Else
        GetConfiguredSavePath = configPath
    End If
    
    DebugLog "GetConfiguredSavePath: Final path: " & GetConfiguredSavePath
End Function

Private Function FolderExists(folderPath As String) As Boolean
    On Error Resume Next
    FolderExists = (Dir(folderPath, vbDirectory) <> "")
    If Err.Number <> 0 Then
        DebugLog "FolderExists: Error checking path '" & folderPath & "': " & Err.Description
        FolderExists = False
    End If
    On Error GoTo 0
End Function

' ==================== WORKSHEET VALIDATION ====================
Private Function SetWorksheetReferences(ByRef wsBatches As Worksheet, _
                                    ByRef wsPLTemplate As Worksheet, _
                                    ByRef wsSCITemplate As Worksheet, _
                                    ByRef errorMessage As String) As Boolean
    On Error Resume Next
    
    DebugLog "SetWorksheetReferences: Setting worksheet references"
    
    Set wsBatches = ThisWorkbook.Sheets("Batches")
    Set wsPLTemplate = ThisWorkbook.Sheets("PL Template - Single Batch")
    Set wsSCITemplate = ThisWorkbook.Sheets("SCI Template - Single Batch")
    
    On Error GoTo 0
    
    If wsBatches Is Nothing Then
        errorMessage = "Batches sheet not found"
        DebugLog "SetWorksheetReferences ERROR: " & errorMessage
        SetWorksheetReferences = False
    ElseIf wsPLTemplate Is Nothing Then
        errorMessage = "PL Template sheet not found"
        DebugLog "SetWorksheetReferences ERROR: " & errorMessage
        SetWorksheetReferences = False
    ElseIf wsSCITemplate Is Nothing Then
        errorMessage = "SCI Template sheet not found"
        DebugLog "SetWorksheetReferences ERROR: " & errorMessage
        SetWorksheetReferences = False
    Else
        DebugLog "SetWorksheetReferences: All worksheets found successfully"
        SetWorksheetReferences = True
    End If
End Function

' ==================== LOGGING FUNCTIONS ====================
Private Sub EnsureLogSheetExists()
    Dim ws As Worksheet
    Dim logExists As Boolean
    
    DebugLog "EnsureLogSheetExists: Checking for log sheet"
    
    For Each ws In ThisWorkbook.Sheets
        If ws.Name = LOG_SHEET_NAME Then
            logExists = True
            DebugLog "EnsureLogSheetExists: Log sheet already exists"
            Exit For
        End If
    Next ws
    
    If Not logExists Then
        DebugLog "EnsureLogSheetExists: Creating new log sheet"
        Set ws = ThisWorkbook.Sheets.Add(After:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count))
        ws.Name = LOG_SHEET_NAME
        
        With ws
            .Range("A1:E1").Value = Array("Timestamp", "Level", "Message", "User", "Duration")
            .Range("A1:E1").Font.Bold = True
            .Range("A1:E1").Interior.ColorIndex = 15
            .Columns("A:A").ColumnWidth = 20
            .Columns("B:B").ColumnWidth = 10
            .Columns("C:C").ColumnWidth = 80
            .Columns("D:D").ColumnWidth = 15
            .Columns("E:E").ColumnWidth = 10
        End With
        DebugLog "EnsureLogSheetExists: Log sheet created with headers"
    End If
End Sub

Private Sub LogMessage(level As String, message As String)
    Dim wsLog As Worksheet
    Dim nextRow As Long
    Static startTime As Date
    
    On Error Resume Next
    Set wsLog = ThisWorkbook.Sheets(LOG_SHEET_NAME)
    On Error GoTo 0
    
    If wsLog Is Nothing Then
        DebugLog "LogMessage: Warning - Log sheet not found"
        Exit Sub
    End If
    
    nextRow = wsLog.Cells(wsLog.Rows.Count, "A").End(xlUp).row + 1
    
    ' Fixed: Correct the log row deletion logic
    If nextRow > MAX_LOG_ROWS Then
        DebugLog "LogMessage: Log rows exceed limit, deleting old entries"
        ' Delete the correct number of rows (keeping header)
        wsLog.Rows("2:" & (MAX_LOG_ROWS \ 2 + 1)).Delete
        nextRow = wsLog.Cells(wsLog.Rows.Count, "A").End(xlUp).row + 1
    End If
    
    If level = "INFO" And InStr(message, "started") > 0 Then
        startTime = Now
    End If
    
    With wsLog
        .Cells(nextRow, 1).Value = Now
        .Cells(nextRow, 2).Value = level
        .Cells(nextRow, 3).Value = message
        .Cells(nextRow, 4).Value = Environ("USERNAME")
        
        If level = "INFO" And InStr(message, "completed") > 0 And startTime > 0 Then
            .Cells(nextRow, 5).Value = Format(Now - startTime, "hh:mm:ss")
        End If
    End With
    
    ' Also log to debug
    DebugLog "LOG [" & level & "]: " & message
End Sub

' ==================== PERFORMANCE OPTIMIZATION ====================
Private Sub OptimizePerformance(turnOn As Boolean)
    DebugLog "OptimizePerformance: " & IIf(turnOn, "Enabling", "Disabling") & " performance mode"
    
    With Application
        .ScreenUpdating = Not turnOn
        .Calculation = IIf(turnOn, xlCalculationManual, xlCalculationAutomatic)
        .EnableEvents = Not turnOn
        .DisplayStatusBar = Not turnOn
        .DisplayAlerts = Not turnOn
    End With
    
    DebugLog "OptimizePerformance: Settings applied"
End Sub

' ==================== ADDITIONAL UTILITIES ====================
Private Function GetRangeWidth(rng As Range) As Double
    Dim totalWidth As Double
    Dim col As Range
    
    totalWidth = 0
    For Each col In rng.Columns
        totalWidth = totalWidth + col.Width
    Next col
    
    GetRangeWidth = totalWidth
End Function

Private Function GetRangeHeight(rng As Range) As Double
    Dim totalHeight As Double
    Dim row As Range
    
    totalHeight = 0
    For Each row In rng.Rows
        totalHeight = totalHeight + row.Height
    Next row
    
    GetRangeHeight = totalHeight
End Function

Private Sub CleanupTemplates()
    Dim wsPL As Worksheet
    Dim wsSCI As Worksheet
    
    DebugLog "CleanupTemplates: Starting template cleanup"
    
    On Error Resume Next
    Set wsPL = ThisWorkbook.Sheets("PL Template - Single Batch")
    Set wsSCI = ThisWorkbook.Sheets("SCI Template - Single Batch")
    On Error GoTo 0
    
    If Not wsPL Is Nothing Then
        wsPL.Range("M10:O10").ClearContents
        wsPL.Range("M110").ClearContents
        DebugLog "CleanupTemplates: Cleaned PL template"
    End If
    
    If Not wsSCI Is Nothing Then
        wsSCI.Range("K8:M8").ClearContents
        wsSCI.Range("M119").ClearContents
        DebugLog "CleanupTemplates: Cleaned SCI template"
    End If
    
    DebugLog "CleanupTemplates: Template cleanup complete"
    MsgBox "Templates cleaned up!", vbInformation
End Sub

' ==================== DEBUG UTILITIES ====================
Sub TestDebugMode()
    ' Test procedure to verify debug logging is working
    DebugLog "TestDebugMode: Debug logging is active"
    DebugLog "TestDebugMode: Current time: " & Now
    DebugLog "TestDebugMode: Username: " & Environ("USERNAME")
    DebugLog "TestDebugMode: Excel version: " & Application.Version
    MsgBox "Check the Immediate window (Ctrl+G) for debug messages", vbInformation
End Sub

Sub ClearDebugWindow()
    ' Clear the Immediate window (works in most cases)
    Debug.Print String(100, vbCrLf)
    DebugLog "Debug window cleared"
End Sub