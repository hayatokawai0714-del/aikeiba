# jvlink_environment_report

- generated_at: 2026-04-29T20:44:12

## System

- os: Windows 10 (10.0.26200)
- python_version: 3.11.4 (tags/v3.11.4:d2340ef, Jun  7 2023, 05:45:37) [MSC v.1934 64 bit (AMD64)]
- python_bits: 64
- dotnet_version: 8.0.420
- pywin32_installed: True

## COM ProgID

| progid | registered | activatable | clsid | inproc_server32 | local_server32 | error |
|---|---|---|---|---|---|---|
| JVDTLab.JVLink | True | False | {2AB1774D-0C41-11D7-916F-0003479BEB3F} |  |  |  |
| JVLink.JVLink | False | False |  |  |  | [WinError 2] 指定されたファイルが見つかりません。 |

## Known Directories

- FOUND: C:\ProgramData\JRA-VAN
- MISSING: C:\Program Files\JRA-VAN
- FOUND: C:\Program Files (x86)\JRA-VAN
- FOUND: C:\Users\HND2205\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\TARGET frontier JV
- FOUND: C:\ProgramData\JRA-VAN\Data Lab

## JV-Link DLL/EXE candidates

- root: C:\Program Files (x86)
  - C:\Program Files (x86)\JRA-VAN\Data Lab\JVLinkAgent.exe

## Guidance

- COM未登録のため JV-Link API を直接呼べません。以下を順に確認してください。
1. JV-Link を再インストールする
2. JV-Link 設定ツール（JRA-VAN DataLab 側）を起動して初期設定を完了する
3. 管理者権限でインストーラ/設定ツールを実行する
4. 32bit/64bit の整合性を確認する（JV-Linkは32bit前提が多い）
5. Python/実行プロセスbit数と JV-Link COM 登録bit数が一致しているか確認する
