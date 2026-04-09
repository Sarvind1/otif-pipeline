Option Explicit

' ==================== CONFIGURATION CONSTANTS ====================
Private Const CONFIG_SHEET_NAME As String = "Config"
Private Const LOG_SHEET_NAME As String = "Process Log"
Private Const SHARED_VIEW_SHEET_NAME As String = "Shared View-Batches - VP"
Private Const MAX_LOG_ROWS As Long = 100000

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

Sub batchUpdate()
    Dim wsBatches As Worksheet
    Dim wsShared As Worksheet
    Dim lastRow As Long
    Dim sourceRange As Range
    
    ' Set references to worksheets
    Set wsBatches = ThisWorkbook.Worksheets("Batches")
    Set wsShared = ThisWorkbook.Worksheets("Shared View-Batches - VP")
    
    ' Clear column A contents in Batches sheet
    wsBatches.Columns("A").ClearContents
    wsBatches.Columns("B").ClearContents
    wsBatches.Columns("C").ClearContents
    
    ' Find last used row in column J on Shared View-Batches - VP sheet
    lastRow = wsShared.Cells(wsShared.Rows.Count, "J").End(xlUp).row
    
    ' Define the source range from J2 down to last used cell in J
    If lastRow >= 2 Then
        Set sourceRange = wsShared.Range("J2:J" & lastRow)
        ' Copy values from sourceRange to A1 on Batches sheet
        wsBatches.Range("A1").Resize(sourceRange.Rows.Count, sourceRange.Columns.Count).Value = sourceRange.Value
    Else
        ' No data to copy if lastRow is less than 2
        MsgBox "No data found in column J starting at row 2 to copy.", vbInformation
    End If
End Sub

Private Function RefreshAndCalculate() As Boolean
    Dim conn As WorkbookConnection
    Dim qt As QueryTable
    Dim lo As ListObject
    Dim ws As Worksheet
    
    On Error GoTo ErrorHandler
    
    Application.ScreenUpdating = False
    
    ' Make ALL data connections synchronous (not background)
    For Each conn In ThisWorkbook.Connections
        conn.BackgroundQuery = False
    Next conn
    
    ' Also check for QueryTables in worksheets
    For Each ws In ThisWorkbook.Worksheets
        For Each qt In ws.QueryTables
            qt.BackgroundQuery = False
        Next qt
        
        ' Check ListObjects (Tables) with queries
        For Each lo In ws.ListObjects
            If Not lo.QueryTable Is Nothing Then
                lo.QueryTable.BackgroundQuery = False
            End If
        Next lo
    Next ws
    
    ' Now RefreshAll will wait for completion
    ThisWorkbook.RefreshAll
    
    ' Re-enable automatic calculation
    Application.Calculation = xlCalculationAutomatic
    Application.CalculateFull
    
    ' Save the workbook`
    ThisWorkbook.Save
    
    Application.ScreenUpdating = True
    RefreshAndCalculate = True
    Exit Function
    
ErrorHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    RefreshAndCalculate = False
End Function


' ==================== MAIN PROCEDURES ====================
Sub ProcessAllBatches()
    Dim stats As ProcessStats
    Dim errorMessage As String
    
    On Error GoTo ErrorHandler
    
    stats.startTime = Now
    Call OptimizePerformance(True)


    Call LogMessage("INFO", "Refreshing all data connections...")
    If RefreshAndCalculate() Then
        Call LogMessage("INFO", "Data refresh completed successfully")
    Else
        Call LogMessage("ERROR", "Data refresh failed")
        Exit Sub
    End If


    Call EnsureLogSheetExists
    Call EnsureSharedViewSheetExists
    Call LogMessage("INFO", "Batch processing started")
    Call batchUpdate
    
    If ProcessBatchesCore(stats, errorMessage) Then
        stats.EndTime = Now
        Call LogMessage("INFO", "Batch processing completed successfully. " & _
                            "Processed: " & stats.TotalBatches & ", " & _
                            "Success: " & stats.SuccessCount & ", " & _
                            "Errors: " & stats.ErrorCount)
        MsgBox "Batch processing completed!" & vbCrLf & _
            "Total: " & stats.TotalBatches & vbCrLf & _
            "Success: " & stats.SuccessCount & vbCrLf & _
            "Errors: " & stats.ErrorCount, vbInformation
    Else
        Call LogMessage("ERROR", "Batch processing failed: " & errorMessage)
        MsgBox "Batch processing failed!" & vbCrLf & errorMessage, vbExclamation
    End If
    
Cleanup:
    Call OptimizePerformance(False)
    Exit Sub
    
ErrorHandler:
    errorMessage = "Unexpected error: " & Err.Description
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
    
    If Not SetWorksheetReferences(wsBatches, wsPLTemplate, wsSCITemplate, errorMessage) Then
        ProcessBatchesCore = False
        Exit Function
    End If
    
    savePath = GetConfiguredSavePath()
    If savePath = "" Then
        errorMessage = "Save path not configured or invalid"
        ProcessBatchesCore = False
        Exit Function
    End If
    
    lastRow = wsBatches.Cells(wsBatches.Rows.Count, "A").End(xlUp).row
    
    For currentRow = 1 To lastRow
        batchValue = Trim(CStr(wsBatches.Cells(currentRow, 1).Value))
        
        If Len(batchValue) > 0 Then
            stats.TotalBatches = stats.TotalBatches + 1
            
            If ProcessSingleBatch(batchValue, currentRow, wsBatches, wsPLTemplate, wsSCITemplate, savePath) Then
                stats.SuccessCount = stats.SuccessCount + 1
            Else
                stats.ErrorCount = stats.ErrorCount + 1
            End If
        End If
    Next currentRow
    
    ProcessBatchesCore = True
    Exit Function
    
ErrorHandler:
    errorMessage = "Error in ProcessBatchesCore: " & Err.Description
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
    
    Call LogMessage("INFO", "Processing batch: " & batchValue)
    
    Call UpdateTemplates(batchValue, wsPLTemplate, wsSCITemplate)
    
    plCINumber = GenerateCINumber(batchValue, "PL")
    sciCINumber = GenerateCINumber(batchValue, "CI")
    
    wsPLTemplate.Range("M110").Value = plCINumber
    wsSCITemplate.Range("M119").Value = sciCINumber
    
    Application.Calculate
    DoEvents
    
    fileName = batchValue & "_RG.xlsx"
    Set wb = CreateBatchWorkbook(batchValue, wsPLTemplate, wsSCITemplate, savePath, fileName)
    
    If Not wb Is Nothing Then
        wsBatches.Cells(rowNum, 2).Value = plCINumber
        wsBatches.Cells(rowNum, 3).Value = sciCINumber
        
        Call AppendToSharedView(batchValue, plCINumber, sciCINumber)
        
        Call LogMessage("INFO", "Successfully created: " & fileName & _
                            " | PL CI: " & plCINumber & " | SCI CI: " & sciCINumber)
        ProcessSingleBatch = True
    Else
        Call LogMessage("ERROR", "Failed to create workbook for batch: " & batchValue)
        ProcessSingleBatch = False
    End If
    
    Exit Function
    
ErrorHandler:
    Call LogMessage("ERROR", "Error processing batch " & batchValue & ": " & Err.Description)
    ProcessSingleBatch = False
End Function

' ==================== CI NUMBER GENERATION ====================
Private Function GenerateCINumber(batchNumber As String, docType As String) As String
    Dim datePrefix As String
    Dim batchSuffix As String
    Dim typeCode As String
    Dim Version As String
    
    datePrefix = Format(Date, "YYYYMMDD")
    batchSuffix = Right(batchNumber, 7)
    Version = "V7"
    
    If docType = "PL" Then
        typeCode = "PL"
    Else
        typeCode = "CI"
    End If
    
    GenerateCINumber = datePrefix & "-" & typeCode & "-" & batchSuffix & "-" & Version
End Function

' ==================== SHARED VIEW MANAGEMENT ====================
Private Sub EnsureSharedViewSheetExists()
    Dim ws As Worksheet
    Dim sheetExists As Boolean
    
    For Each ws In ThisWorkbook.Sheets
        If ws.Name = SHARED_VIEW_SHEET_NAME Then
            sheetExists = True
            Exit For
        End If
    Next ws
    
    If Not sheetExists Then
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
    End If
End Sub

Private Sub AppendToSharedView(batchNumber As String, plKey As String, ciKey As String)
    Dim wsShared As Worksheet
    Dim nextRow As Long
    Dim serialNumber As Long
    
    On Error Resume Next
    Set wsShared = ThisWorkbook.Sheets(SHARED_VIEW_SHEET_NAME)
    On Error GoTo 0
    
    If wsShared Is Nothing Then Exit Sub
    
    nextRow = wsShared.Cells(wsShared.Rows.Count, "A").End(xlUp).row + 1
    
    If nextRow = 2 Then
        serialNumber = 1
    Else
        serialNumber = val(wsShared.Cells(nextRow - 1, 1).Value) + 1
    End If
    
    With wsShared
        .Cells(nextRow, 1).Value = serialNumber
        .Cells(nextRow, 2).Value = Date
        .Cells(nextRow, 3).Value = batchNumber
        .Cells(nextRow, 4).Value = batchNumber
        .Cells(nextRow, 5).Value = plKey
        .Cells(nextRow, 6).Value = ciKey
    End With
End Sub

' ==================== TEMPLATE HANDLING ====================
Private Sub UpdateTemplates(batchValue As String, wsPL As Worksheet, wsSCI As Worksheet)
    wsPL.Range("M10:O10").ClearContents
    wsSCI.Range("K8:M8").ClearContents
    
    wsPL.Range("M110").ClearContents
    wsSCI.Range("M119").ClearContents
    
    wsPL.Range("M10").Value = batchValue
    wsSCI.Range("K9").Value = batchValue
End Sub

Private Function CreateBatchWorkbook(batchName As String, wsPLTemplate As Worksheet, _
                                wsSCITemplate As Worksheet, savePath As String, _
                                fileName As String) As Workbook
    Dim wb As Workbook
    Dim wsPL As Worksheet, wsCI As Worksheet
    Dim fullPath As String
    
    On Error GoTo ErrorHandler
    
    Application.DisplayAlerts = False
    
    Set wb = Workbooks.Add(xlWBATWorksheet)
    wb.Sheets(1).Name = "PL"
    wb.Sheets.Add(After:=wb.Sheets(1)).Name = "CI"
    
    Set wsPL = wb.Sheets("PL")
    Set wsCI = wb.Sheets("CI")
    
    Call ProcessPLSheet(wsPLTemplate, wsPL)
    Call ProcessCISheet(wsSCITemplate, wsCI)
    
    ' Apply consistent print settings to both sheets
    Call ApplyConsistentPrintSettings(wsPL, wsCI)
    
    fullPath = savePath & Application.PathSeparator & fileName
    wb.SaveAs fullPath, FileFormat:=xlOpenXMLWorkbook
    wb.Close SaveChanges:=False
    
    Set CreateBatchWorkbook = wb
    Application.DisplayAlerts = True
    Exit Function
    
ErrorHandler:
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

    lastRow = FindLastRow(sourceSheet)
    lastCol = FindLastColumn(sourceSheet)

    ReDim sourceRowHeights(1 To lastRow)
    For i = 1 To lastRow
        sourceRowHeights(i) = sourceSheet.Rows(i).RowHeight
    Next i

    With sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol))
        targetSheet.Range("A1").Resize(.Rows.Count, .Columns.Count).Value = .Value
    End With

    For i = 1 To lastRow
        targetSheet.Rows(i).RowHeight = sourceRowHeights(i)
    Next i

    sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol)).Copy
    targetSheet.Range("A1").PasteSpecial xlPasteFormats
    targetSheet.Range("A1").PasteSpecial xlPasteColumnWidths

    'CopyMergedCells sourceSheet, targetSheet, lastRow, lastCol

    CopyFormulas sourceSheet.Range("I99:O99"), targetSheet.Range("I99:O99")
    CopyFormulas sourceSheet.Range("D108:D111"), targetSheet.Range("D108:D111")

    Application.Calculate
    targetSheet.Calculate
    DoEvents
    Call AdjustProductNames(targetSheet.Range("E23:E98"))
    
    ' AutoFit rows 23:98 BEFORE deleting blank rows
    targetSheet.Range("23:98").EntireRow.AutoFit
    
    Call DeleteBlankRowsInRange(targetSheet.Range("A23:A98"))

    ' AutoFit columns for the entire data range
    With targetSheet.Range(targetSheet.Cells(1, 1), targetSheet.Cells(lastRow, lastCol))
        .EntireColumn.AutoFit  ' AutoFit columns for optimal width
    End With
    
    targetSheet.Columns("J:L").ColumnWidth = 10

    Application.CutCopyMode = False
End Sub

Private Sub ProcessCISheet(sourceSheet As Worksheet, targetSheet As Worksheet)
    Dim lastRow As Long
    Dim lastCol As Long
    Dim formulaRanges As Variant
    Dim i As Long
    Dim sourceRowHeights() As Double  ' Already added in your code

    lastRow = FindLastRow(sourceSheet)
    lastCol = FindLastColumn(sourceSheet)

    ReDim sourceRowHeights(1 To lastRow)
    For i = 1 To lastRow
        sourceRowHeights(i) = sourceSheet.Rows(i).RowHeight
    Next i

    With sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol))
        targetSheet.Range("A1").Resize(.Rows.Count, .Columns.Count).Value = .Value
    End With

    For i = 1 To lastRow
        targetSheet.Rows(i).RowHeight = sourceRowHeights(i)
    Next i

    sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(lastRow, lastCol)).Copy
    targetSheet.Range("A1").PasteSpecial xlPasteFormats
    targetSheet.Range("A1").PasteSpecial xlPasteColumnWidths

    CopyMergedCells sourceSheet, targetSheet, lastRow, lastCol

    CopyFormulas sourceSheet.Range("H99:M106"), targetSheet.Range("H99:M106")

    formulaRanges = Array("B117", "B118", "B123", "B124")
    For i = 0 To UBound(formulaRanges)
        CopyFormulas sourceSheet.Range(formulaRanges(i)), targetSheet.Range(formulaRanges(i))
    Next i

    Application.Calculate
    targetSheet.Calculate
    DoEvents

    Call AdjustProductNames(targetSheet.Range("E23:E98"))
    targetSheet.Range("23:98").EntireRow.AutoFit
    Call DeleteBlankRowsInRange(targetSheet.Range("A23:A98"))

    ' AutoFit columns for the entire data range
    With targetSheet.Range(targetSheet.Cells(1, 1), targetSheet.Cells(lastRow, lastCol))
        .EntireColumn.AutoFit  ' AutoFit columns for optimal width
    End With
    
    
    targetSheet.Columns("H:I").ColumnWidth = 10
    targetSheet.Columns("M:M").ColumnWidth = 25
    Application.CutCopyMode = False
End Sub

' ==================== IMPROVED PRINT SETTINGS ====================
Private Sub ApplyConsistentPrintSettings(wsPL As Worksheet, wsCI As Worksheet)
    On Error Resume Next
    
    Dim printInfo As PrintOrientationInfo
    
    ' Determine the best orientation for both sheets
    printInfo = DetermineBestOrientation(wsPL, wsCI)
    
    ' Apply the same print settings to both sheets
    Call ApplyPrintSettingsToSheet(wsPL, printInfo)
    Call ApplyPrintSettingsToSheet(wsCI, printInfo)
    
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
    
    ' Get used ranges
    Set plUsedRange = wsPL.usedRange
    Set ciUsedRange = wsCI.usedRange
    
    ' Calculate content dimensions for both sheets
    plWidth = GetRangeWidth(plUsedRange)
    plHeight = GetRangeHeight(plUsedRange)
    ' ciWidth = GetRangeWidth(ciUsedRange)
    ' ciHeight = GetRangeHeight(ciUsedRange)
    
    ' Use the maximum dimensions to ensure both sheets fit
    maxWidth = plWidth
    maxHeight = plHeight
    
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
    
    ' Decision logic: Choose orientation that provides better fit
    ' Priority: 1) Fits on one page, 2) Better vertical space utilization

        If portraitVerticalUsage > 0.6 Then
            ' Portrait uses good amount of vertical space
            result.orientation = xlPortrait
            result.fitToWidth = 1
        Else
            ' Both have poor utilization, choose portrait as default
            result.orientation = xlLandscape
            result.fitToHeight = 1
        End If
    
    DetermineBestOrientation = result
End Function

Private Sub ApplyPrintSettingsToSheet(ws As Worksheet, printInfo As PrintOrientationInfo)
    On Error Resume Next

    Dim usedRange As Range
    Set usedRange = ws.usedRange

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

    On Error GoTo 0
End Sub


' ==================== UTILITY FUNCTIONS ====================
Private Sub CopyMergedCells(sourceSheet As Worksheet, targetSheet As Worksheet, maxRow As Long, maxCol As Long)
    Dim cell As Range
    Dim mergeArea As Range
    
    On Error Resume Next
    
    For Each cell In sourceSheet.Range(sourceSheet.Cells(1, 1), sourceSheet.Cells(maxRow, maxCol))
        If cell.MergeCells Then
            Set mergeArea = cell.mergeArea
            If cell.Address = mergeArea.Cells(1, 1).Address Then
                targetSheet.Range(mergeArea.Address).Merge
            End If
        End If
    Next cell
    
    On Error GoTo 0
End Sub

Private Sub CopyFormulas(sourceRange As Range, targetRange As Range)
    On Error Resume Next
    targetRange.Formula = sourceRange.Formula
    On Error GoTo 0
End Sub

Private Sub DeleteBlankRowsInRange(rng As Range)
    Dim rowsToDelete As Range
    Dim cell As Range
    
    For Each cell In rng
        If IsEmpty(cell.Value) Then
            If rowsToDelete Is Nothing Then
                Set rowsToDelete = cell.EntireRow
            Else
                Set rowsToDelete = Union(rowsToDelete, cell.EntireRow)
            End If
        End If
    Next cell
    
    If Not rowsToDelete Is Nothing Then
        rowsToDelete.Delete
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
    
    maxLines = 3
    originalHeight = 18
    maxCharsPerLine = 32
    
    For Each cell In rng
        If Not IsEmpty(cell.Value) Then
            cellText = CStr(cell.Value)
            totalMaxChars = maxCharsPerLine * maxLines

            requiredLines = Application.WorksheetFunction.RoundUp(Len(cellText) / maxCharsPerLine, 0)
            
            cell.WrapText = True
            
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
            End Select
            
            If requiredLines > 1 Then
                cell.RowHeight = cell.RowHeight + 2
            End If
            
            cell.VerticalAlignment = xlTop
        End If
    Next cell
End Sub

Private Function FindLastRow(ws As Worksheet) As Long
    Dim lastRow As Long
    On Error Resume Next
    lastRow = ws.Cells.Find(What:="*", SearchOrder:=xlByRows, SearchDirection:=xlPrevious).row
    On Error GoTo 0
    If lastRow = 0 Then lastRow = 1
    If lastRow > 150 Then lastRow = 150
    FindLastRow = lastRow
End Function

Private Function FindLastColumn(ws As Worksheet) As Long
    Dim lastCol As Long
    On Error Resume Next
    lastCol = ws.Cells.Find(What:="*", SearchOrder:=xlByColumns, SearchDirection:=xlPrevious).Column
    On Error GoTo 0
    If lastCol = 0 Then lastCol = 26
    If lastCol > 26 Then lastCol = 26
    FindLastColumn = lastCol
End Function

' ==================== CONFIGURATION FUNCTIONS ====================
Private Function GetConfiguredSavePath() As String
    Dim configSheet As Worksheet
    Dim configPath As String
    Dim defaultPath As String
    
    On Error Resume Next
    Set configSheet = ThisWorkbook.Sheets(CONFIG_SHEET_NAME)
    On Error GoTo 0
    
    If Not configSheet Is Nothing Then
        configPath = Trim(CStr(configSheet.Range("B2").Value))
    End If
    
    #If Mac Then
        Const isMac = True
    #Else
        Const isMac = False
    #End If
        
    Debug.Print isMac
    
    If configPath = "" Or Not FolderExists(configPath) Then

        #If Mac Then
            defaultPath = "/Users/" & "teq-admin" & "/Documents/Shipment documents"
        #Else
            defaultPath = Environ("USERPROFILE") & "\Documents\Shipment documents"
        #End If
        
        If FolderExists(defaultPath) Then
            GetConfiguredSavePath = defaultPath
        Else
            On Error Resume Next
            MkDir defaultPath
            On Error GoTo 0
            
            If FolderExists(defaultPath) Then
                GetConfiguredSavePath = defaultPath
            Else
                GetConfiguredSavePath = Application.DefaultFilePath
            End If
        End If
    Else
        GetConfiguredSavePath = configPath
    End If
    Debug.Print configPath, defaultPath, GetConfiguredSavePath, Application.DefaultFilePath
End Function

Private Function FolderExists(folderPath As String) As Boolean
    On Error Resume Next
    FolderExists = (Dir(folderPath, vbDirectory) <> "")
    On Error GoTo 0
End Function

' ==================== WORKSHEET VALIDATION ====================
Private Function SetWorksheetReferences(ByRef wsBatches As Worksheet, _
                                    ByRef wsPLTemplate As Worksheet, _
                                    ByRef wsSCITemplate As Worksheet, _
                                    ByRef errorMessage As String) As Boolean
    On Error Resume Next
    
    Set wsBatches = ThisWorkbook.Sheets("Batches")
    Set wsPLTemplate = ThisWorkbook.Sheets("PL Template - Single Batch")
    Set wsSCITemplate = ThisWorkbook.Sheets("SCI Template - Single Batch")
    
    On Error GoTo 0
    
    If wsBatches Is Nothing Then
        errorMessage = "Batches sheet not found"
        SetWorksheetReferences = False
    ElseIf wsPLTemplate Is Nothing Then
        errorMessage = "PL Template sheet not found"
        SetWorksheetReferences = False
    ElseIf wsSCITemplate Is Nothing Then
        errorMessage = "SCI Template sheet not found"
        SetWorksheetReferences = False
    Else
        SetWorksheetReferences = True
    End If
End Function

' ==================== LOGGING FUNCTIONS ====================
Private Sub EnsureLogSheetExists()
    Dim ws As Worksheet
    Dim logExists As Boolean
    
    For Each ws In ThisWorkbook.Sheets
        If ws.Name = LOG_SHEET_NAME Then
            logExists = True
            Exit For
        End If
    Next ws
    
    If Not logExists Then
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
    End If
End Sub

Private Sub LogMessage(level As String, message As String)
    Dim wsLog As Worksheet
    Dim nextRow As Long
    Static startTime As Date
    
    On Error Resume Next
    Set wsLog = ThisWorkbook.Sheets(LOG_SHEET_NAME)
    On Error GoTo 0
    
    If wsLog Is Nothing Then Exit Sub
    
    nextRow = wsLog.Cells(wsLog.Rows.Count, "A").End(xlUp).row + 1
    
    If nextRow > MAX_LOG_ROWS Then
        wsLog.Rows("2:50001").Delete
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
End Sub

' ==================== PERFORMANCE OPTIMIZATION ====================
Private Sub OptimizePerformance(turnOn As Boolean)
    With Application
        .ScreenUpdating = Not turnOn
        .Calculation = IIf(turnOn, xlCalculationManual, xlCalculationAutomatic)
        .EnableEvents = Not turnOn
        .DisplayStatusBar = Not turnOn
        .DisplayAlerts = Not turnOn
    End With
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
        totalHeight = totalHeight + row.RowHeight
    Next row
    
    GetRangeHeight = totalHeight
End Function

Private Sub CleanupTemplates()
    Dim wsPL As Worksheet
    Dim wsSCI As Worksheet
    
    On Error Resume Next
    Set wsPL = ThisWorkbook.Sheets("PL Template - Single Batch")
    Set wsSCI = ThisWorkbook.Sheets("SCI Template - Single Batch")
    On Error GoTo 0
    
    If Not wsPL Is Nothing Then
        wsPL.Range("M10:O10").ClearContents
        wsPL.Range("M110").ClearContents
    End If
    
    If Not wsSCI Is Nothing Then
        wsSCI.Range("K8:M8").ClearContents
        wsSCI.Range("M119").ClearContents
    End If
    
    MsgBox "Templates cleaned up!", vbInformation
End Sub



/Users/teq-admin/Downloads/otif_autom_0829/enhanced_code.vb