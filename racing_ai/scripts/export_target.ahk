#NoEnv
#SingleInstance Force
SendMode, Input
SetTitleMatchMode, 2
CoordMode, Mouse, Screen
SetWorkingDir, %A_ScriptDir%

; TARGET external index export (AHK v1.1)

argc = %0%
arg1 = %1%
arg2 = %2%
arg3 = %3%
arg4 = %4%
arg5 = %5%
arg6 = %6%
arg7 = %7%

debugLogPath := "C:\JV-Data\test\export_target_debug.log"
FileCreateDir, C:\JV-Data\test
FileDelete, %debugLogPath%

if (argc < 2) {
    ShowError("Missing args.`nUsage: export_target.ahk YYYY-MM-DD out_dir [target_exe_path] [--only races] [--manual-menu] [--silent]")
    ExitApp, 1
}

todayDate = %1%
outDir = %2%
targetExePath =
onlyMode = all
manualMenuMode = 0
silentMode = 0

if (argc >= 3) {
    if (arg3 = "--only")
        onlyMode = %4%
    else
        targetExePath = %3%
}
if (argc >= 5) {
    if (arg4 = "--only")
        onlyMode = %5%
}
if (arg5 = "--only")
    onlyMode = %6%
if (arg6 = "--only")
    onlyMode = %7%

if (arg3 = "--manual-menu" || arg4 = "--manual-menu" || arg5 = "--manual-menu" || arg6 = "--manual-menu" || arg7 = "--manual-menu")
    manualMenuMode = 1
if (arg3 = "--silent" || arg4 = "--silent" || arg5 = "--silent" || arg6 = "--silent" || arg7 = "--silent")
    silentMode = 1

StringLower, onlyMode, onlyMode
if (onlyMode = "")
    onlyMode = all
if !(onlyMode = "all" || onlyMode = "races") {
    ShowError("Only races mode is supported. Use --only races")
    ExitApp, 2
}

FileCreateDir, %outDir%
configPath := A_ScriptDir . "\export_target_config.json"
completeDialogTimeoutSec := 20
manualMenuTimeoutSec := 180
FormatTime, runStartTs,, yyyyMMddHHmmss

LogDebug("script_start argc=" . argc . " target_date=" . todayDate . " out_dir=" . outDir . " only_mode=" . onlyMode . " manual_menu_mode=" . manualMenuMode . " config_path=" . configPath)
LogDebug("run_start_ts=" . runStartTs)

if !FileExist(configPath) {
    LogDebug("error config_not_found")
    ShowError("Config not found: " . configPath)
    ExitApp, 3
}

configText := ""
FileRead, configText, %configPath%
if ErrorLevel {
    LogDebug("error config_read_failed")
    ShowError("Failed to read config: " . configPath)
    ExitApp, 4
}
LogDebug("config_read_ok")

ParsePoint(configText, "race_button", raceX, raceY)
ParsePoint(configText, "race_select_ok_button", raceSelectOkX, raceSelectOkY)
ParsePoint(configText, "file_menu", fileMenuX, fileMenuY)
ParsePoint(configText, "complete_ok_button", completeOkX, completeOkY)

targetTitle := FindTargetWindowTitle()
if (targetTitle = "") {
    LogDebug("error target_not_found")
    ShowError("TARGET window not found. Open TARGET first.")
    ExitApp, 5
}
LogDebug("target_found title=" . targetTitle)
WinGet, targetProcessName, ProcessName, %targetTitle%
LogDebug("target_process_name=" . targetProcessName)

if !ActivateTargetWindow(targetTitle) {
    LogDebug("error target_activate_failed")
    ShowError("Failed to activate TARGET.")
    ExitApp, 6
}
LogDebug("active_ok")

; 1) race button
ClickStep(targetTitle, "race_button", raceX, raceY, 450)

; 2) no dialog detection: fixed coordinate click
Sleep, 1500
LogDebug("race_select_ok_click_without_wait x=" . raceSelectOkX . " y=" . raceSelectOkY)
ClickAt("race_select_ok_button", raceSelectOkX, raceSelectOkY, 300)
Sleep, 1500

foundWindowTitle =
activeWindowTitle =
dialogFound := false
completionMode =
foundFilePath =

if (manualMenuMode = 1) {
    LogDebug("manual_menu_mode=1 waiting_user_operation")
    ClickStep(targetTitle, "file_menu", fileMenuX, fileMenuY, 250)
    TrayTip, export_target, Manual mode: select External Index Output > All races in TARGET., 5, 1
    dialogFound := WaitForCompletion(manualMenuTimeoutSec, targetTitle, targetProcessName, outDir, runStartTs, completionMode, foundWindowTitle, activeWindowTitle, foundFilePath)
    LogDebug("manual_menu_result wait_complete_dialog_timeout_sec=" . manualMenuTimeoutSec . " complete_dialog_found=" . (dialogFound ? "1" : "0") . " completion_mode=" . completionMode . " found_window_title=" . foundWindowTitle . " active_window_title=" . activeWindowTitle . " found_file_path=" . foundFilePath)
} else {
    ClickStep(targetTitle, "file_menu", fileMenuX, fileMenuY, 350)
    if !ActivateTargetWindow(targetTitle) {
        LogDebug("error activate_failed step=access_key_menu")
        ShowError("Failed to activate TARGET before access-key menu operation.")
        ExitApp, 9
    }
    LogDebug("step_begin access_key_nav")
    Send, !f
    Sleep, 250
    Send, x
    Sleep, 250
    Send, a
    Sleep, 250
    Send, {Enter}
    Sleep, 400
    LogDebug("step_end access_key_nav")
    dialogFound := WaitForCompletion(completeDialogTimeoutSec, targetTitle, targetProcessName, outDir, runStartTs, completionMode, foundWindowTitle, activeWindowTitle, foundFilePath)
    LogDebug("access_key_result wait_complete_dialog_timeout_sec=" . completeDialogTimeoutSec . " complete_dialog_found=" . (dialogFound ? "1" : "0") . " completion_mode=" . completionMode . " found_window_title=" . foundWindowTitle . " active_window_title=" . activeWindowTitle . " found_file_path=" . foundFilePath)
}

if !dialogFound {
    LogDebug("error menu_select_failed")
    ShowError("Failed to select External Index menu.")
    ExitApp, 1
}

if (completionMode = "dialog") {
    ClickAt("complete_ok_button", completeOkX, completeOkY, 400)
} else {
    LogDebug("complete_ok_button_skip completion_mode=" . completionMode)
}
LogDebug("export_done")
LogDebug("completed_successfully")
ShowInfo("Races export flow completed.")
ExitApp, 0

; ---------- helpers ----------

LogDebug(text) {
    global debugLogPath
    FormatTime, nowStr,, yyyy-MM-dd HH:mm:ss
    FileAppend, [%nowStr%] %text%`r`n, %debugLogPath%
}

ParsePoint(configText, key, ByRef x, ByRef y) {
    pattern := """" . key . """\s*:\s*\[\s*([0-9]+)\s*,\s*([0-9]+)\s*\]"
    RegExMatch(configText, pattern, m)
    if (m1 = "" || m2 = "") {
        LogDebug("error coordinate_parse_failed key=" . key)
        ShowError("Invalid or missing coordinate key: " . key)
        ExitApp, 20
    }
    x := m1 + 0
    y := m2 + 0
    LogDebug("coordinate_ok key=" . key . " x=" . x . " y=" . y)
}

FindTargetWindowTitle() {
    WinGet, idList, List
    bestTitle =
    Loop, %idList%
    {
        thisId := idList%A_Index%
        WinGetTitle, title, ahk_id %thisId%
        if (title = "")
            continue
        if (InStr(title, "export_target.ahk") || InStr(title, "export_target"))
            continue
        if (InStr(title, "TARGET frontier JV"))
            return title
        if (InStr(title, "TARGET frontier"))
            bestTitle := title
    }
    if (bestTitle != "")
        return bestTitle
    return ""
}

ActivateTargetWindow(targetTitle) {
    WinActivate, %targetTitle%
    WinWaitActive, %targetTitle%,, 5
    if ErrorLevel
        return false
    return true
}

ClickAt(stepName, x, y, sleepMs) {
    LogDebug("step_begin " . stepName . " x=" . x . " y=" . y)
    Click, %x%, %y%
    Sleep, %sleepMs%
    LogDebug("step_end " . stepName)
}

ClickStep(targetTitle, stepName, x, y, sleepMs) {
    if !ActivateTargetWindow(targetTitle) {
        LogDebug("error activate_failed step=" . stepName)
        ShowError("Failed to activate TARGET at step: " . stepName)
        ExitApp, 21
    }
    ClickAt(stepName, x, y, sleepMs)
}

GetNewestOutputFile(outDir, ByRef newestFilePath, ByRef newestFileTime, ByRef newestFileSize) {
    global debugLogPath
    newestFilePath =
    newestFileTime =
    newestFileSize = 0
    Loop, Files, %outDir%\*.*, F
    {
        filePath := A_LoopFileLongPath
        if (filePath = debugLogPath)
            continue
        SplitPath, filePath, fileName, fileDir, fileExt
        StringLower, fileExt, fileExt
        if (fileExt = "log")
            continue
        FileGetTime, t, %filePath%, M
        FileGetSize, s, %filePath%
        if (t = "")
            continue
        if (newestFileTime = "" || t > newestFileTime) {
            newestFileTime := t
            newestFilePath := filePath
            newestFileSize := s
        }
    }
}

WaitForCompletion(timeoutSec, targetTitle, targetProcessName, outDir, runStartTs, ByRef completionMode, ByRef foundWindowTitle, ByRef activeWindowTitle, ByRef foundFilePath) {
    completionMode =
    foundWindowTitle =
    activeWindowTitle =
    foundFilePath =
    endTick := A_TickCount + (timeoutSec * 1000)
    nextPulseTick := A_TickCount + 5000
    processingSeen := false
    processingSeenTick := 0
    LogDebug("wait_complete_dialog_timeout_sec=" . timeoutSec)
    Loop {
        WinGetTitle, nowActiveTitle, A
        activeWindowTitle := nowActiveTitle
        if (InStr(activeWindowTitle, "しばらくお待ちください") || InStr(activeWindowTitle, "JRAターゲットデータ取得")) {
            if !processingSeen {
                processingSeen := true
                processingSeenTick := A_TickCount
                LogDebug("processing_seen_from_active_title title=" . activeWindowTitle)
            }
        }

        newestFile =
        newestTs =
        newestSize = 0
        GetNewestOutputFile(outDir, newestFile, newestTs, newestSize)
        if (newestTs != "" && newestTs >= runStartTs && newestSize > 0) {
            foundFilePath := newestFile
            completionMode := "file"
            return true
        }

        WinGet, idList, List
        Loop, %idList%
        {
            thisId := idList%A_Index%
            WinGetClass, klass, ahk_id %thisId%
            WinGet, procName, ProcessName, ahk_id %thisId%
            WinGetTitle, title, ahk_id %thisId%
            if (procName = "AutoHotkey.exe")
                continue

            if (InStr(title, "export_target.ahk") || InStr(title, "export_target"))
                continue

            if (klass = "#32770" && IsTargetProcess(procName, targetProcessName)) {
                WinGetText, dialogText, ahk_id %thisId%
                if (InStr(title, "しばらくお待ちください") || InStr(dialogText, "しばらくお待ちください")) {
                    if !processingSeen {
                        processingSeen := true
                        processingSeenTick := A_TickCount
                        LogDebug("processing_dialog_seen title=" . title)
                    }
                }
                if IsCompletionDialog(title, dialogText) {
                    foundWindowTitle := title
                    WinActivate, ahk_id %thisId%
                    WinWaitActive, ahk_id %thisId%,, 1
                    completionMode := "dialog"
                    return true
                } else {
                    LogDebug("dialog_candidate_rejected title=" . title)
                }
            }

            WinGetText, titleText, ahk_id %thisId%
            if IsCompletionDialog(title, titleText) {
                foundWindowTitle := title
                completionMode := "dialog"
                return true
            }

            if (title != "") {
                if (InStr(title, "Race") || InStr(title, "Select"))
                    continue
                if (InStr(title, "Complete") || InStr(title, "Output") || InStr(title, "Done")) {
                    foundWindowTitle := title
                    WinActivate, ahk_id %thisId%
                    WinWaitActive, ahk_id %thisId%,, 1
                    completionMode := "dialog"
                    return true
                }
            }
        }

        if (processingSeen && !HasProcessingWindow()) {
            if (A_TickCount - processingSeenTick >= 2000) {
                foundWindowTitle := "processing_completed_without_dialog"
                completionMode := "processing"
                return true
            }
        }

        if (A_TickCount >= nextPulseTick) {
            LogDebug("wait_pulse active_window_title=" . activeWindowTitle . " target_title=" . targetTitle)
            nextPulseTick := A_TickCount + 5000
        }

        if (A_TickCount >= endTick)
            return false
        Sleep, 250
    }
}

IsCompletionDialog(title, dialogText) {
    if (InStr(title, "出馬表ファイル選択") || InStr(title, "対象レース"))
        return false
    if (InStr(dialogText, "出馬表ファイル選択") || InStr(dialogText, "対象レース"))
        return false

    if (InStr(title, "完了") || InStr(title, "出力") || InStr(title, "レースの出力"))
        return true
    if (InStr(dialogText, "完了") || InStr(dialogText, "出力") || InStr(dialogText, "外部指数") || InStr(dialogText, "レースの出力"))
        return true
    if (InStr(title, "Complete") || InStr(title, "Output") || InStr(title, "Done"))
        return true
    if (InStr(dialogText, "Complete") || InStr(dialogText, "Output") || InStr(dialogText, "Done"))
        return true
    return false
}

IsTargetProcess(procName, expectedProcName) {
    if (procName = expectedProcName)
        return true
    StringUpper, p, procName
    if (p = "TFJV.EXE" || p = "TFPOP.EXE" || p = "TARGET.EXE")
        return true
    return false
}

HasProcessingWindow() {
    WinGet, idList, List
    Loop, %idList%
    {
        thisId := idList%A_Index%
        WinGetTitle, title, ahk_id %thisId%
        if !InStr(title, "しばらくお待ちください")
            continue
        WinGetClass, klass, ahk_id %thisId%
        if (klass = "#32770")
            return true
    }
    return false
}

ShowError(message) {
    global silentMode
    LogDebug("show_error " . message)
    if (silentMode = 1)
        return
    MsgBox, 16, export_target, %message%
}

ShowInfo(message) {
    global silentMode
    LogDebug("show_info " . message)
    if (silentMode = 1)
        return
    MsgBox, 64, export_target, %message%
}
