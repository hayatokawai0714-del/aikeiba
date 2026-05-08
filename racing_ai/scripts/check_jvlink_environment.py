from __future__ import annotations

import argparse
import importlib.util
import platform
import struct
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import winreg  # type: ignore
except Exception:  # pragma: no cover
    winreg = None  # type: ignore


PROGIDS = ["JVDTLab.JVLink", "JVLink.JVLink"]
DLL_EXE_PATTERNS = [
    "*JVLink*.dll",
    "*JVLink*.exe",
    "*JVDTLab*.dll",
    "*JVDTLab*.exe",
]
DIR_CANDIDATES = [
    Path(r"C:\ProgramData\JRA-VAN"),
    Path(r"C:\Program Files\JRA-VAN"),
    Path(r"C:\Program Files (x86)\JRA-VAN"),
    Path(r"C:\Users\HND2205\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\TARGET frontier JV"),
    Path(r"C:\ProgramData\JRA-VAN\Data Lab"),
]


@dataclass
class ProgIdStatus:
    progid: str
    registered: bool
    activatable: bool
    clsid: str | None
    inproc_server32: str | None
    local_server32: str | None
    error: str | None


def run_cmd(args: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
    return p.returncode, p.stdout, p.stderr


def check_progid(progid: str) -> ProgIdStatus:
    if winreg is None:
        return ProgIdStatus(progid, False, False, None, None, None, "winreg_unavailable")
    try:
        clsid = None
        inproc = None
        local = None
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, fr"{progid}\CLSID") as key:
            clsid = winreg.QueryValueEx(key, "")[0]
        if clsid:
            try:
                with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, fr"CLSID\{clsid}\InprocServer32") as key:
                    inproc = winreg.QueryValueEx(key, "")[0]
            except Exception:
                pass
            try:
                with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, fr"CLSID\{clsid}\LocalServer32") as key:
                    local = winreg.QueryValueEx(key, "")[0]
            except Exception:
                pass
        activatable = bool((inproc or "").strip() or (local or "").strip())
        return ProgIdStatus(progid, True, activatable, clsid, inproc, local, None)
    except Exception as e:
        return ProgIdStatus(progid, False, False, None, None, None, str(e))


def find_candidates() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    search_roots = [
        Path(r"C:\ProgramData"),
        Path(r"C:\Program Files"),
        Path(r"C:\Program Files (x86)"),
        Path(r"C:\Users\HND2205\AppData\Local"),
        Path(r"C:\Users\HND2205\AppData\Roaming"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        hits: list[str] = []
        for pat in DLL_EXE_PATTERNS:
            try:
                for p in root.rglob(pat):
                    if p.is_file():
                        hits.append(str(p))
                        if len(hits) >= 80:
                            break
            except Exception:
                continue
            if len(hits) >= 80:
                break
        if hits:
            out[str(root)] = sorted(set(hits))
    return out


def write_report(path: Path, data: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# jvlink_environment_report")
    lines.append("")
    lines.append(f"- generated_at: {data['generated_at']}")
    lines.append("")
    lines.append("## System")
    lines.append("")
    lines.append(f"- os: {data['os']}")
    lines.append(f"- python_version: {data['python_version']}")
    lines.append(f"- python_bits: {data['python_bits']}")
    lines.append(f"- dotnet_version: {data['dotnet_version']}")
    lines.append(f"- pywin32_installed: {data['pywin32_installed']}")
    lines.append("")
    lines.append("## COM ProgID")
    lines.append("")
    lines.append("| progid | registered | activatable | clsid | inproc_server32 | local_server32 | error |")
    lines.append("|---|---|---|---|---|---|---|")
    for s in data["progid_status"]:
        lines.append(
            f"| {s.progid} | {s.registered} | {s.activatable} | {s.clsid or ''} | {s.inproc_server32 or ''} | {s.local_server32 or ''} | {(s.error or '').replace('|','/')} |"
        )
    lines.append("")
    lines.append("## Known Directories")
    lines.append("")
    for d in data["dir_candidates"]:
        lines.append(f"- {'FOUND' if d['exists'] else 'MISSING'}: {d['path']}")
    lines.append("")
    lines.append("## JV-Link DLL/EXE candidates")
    lines.append("")
    if not data["dll_exe_candidates"]:
        lines.append("- (none)")
    else:
        for root, files in data["dll_exe_candidates"].items():
            lines.append(f"- root: {root}")
            for fp in files[:30]:
                lines.append(f"  - {fp}")
            if len(files) > 30:
                lines.append(f"  - ... ({len(files)-30} more)")
    lines.append("")
    lines.append("## Guidance")
    lines.append("")
    if not any(s.activatable for s in data["progid_status"]):
        lines.append("- COM未登録のため JV-Link API を直接呼べません。以下を順に確認してください。")
        lines.append("1. JV-Link を再インストールする")
        lines.append("2. JV-Link 設定ツール（JRA-VAN DataLab 側）を起動して初期設定を完了する")
        lines.append("3. 管理者権限でインストーラ/設定ツールを実行する")
        lines.append("4. 32bit/64bit の整合性を確認する（JV-Linkは32bit前提が多い）")
        lines.append("5. Python/実行プロセスbit数と JV-Link COM 登録bit数が一致しているか確認する")
    else:
        lines.append("- ProgID登録は確認できています。次は実データ取得テスト（1日）を行ってください。")
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Diagnose JV-Link COM/DataLab environment without modifying system.")
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/jvlink_environment_report.md"))
    args = ap.parse_args()

    dotnet_rc, dotnet_out, dotnet_err = run_cmd(["dotnet", "--version"])
    dotnet_version = dotnet_out.strip() if dotnet_rc == 0 else f"unavailable ({dotnet_err.strip()})"

    progid_status = [check_progid(p) for p in PROGIDS]
    dirs = [{"path": str(d), "exists": d.exists()} for d in DIR_CANDIDATES]
    candidates = find_candidates()

    data: dict[str, Any] = {
        "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "os": f"{platform.system()} {platform.release()} ({platform.version()})",
        "python_version": sys.version.replace("\n", " "),
        "python_bits": struct.calcsize("P") * 8,
        "dotnet_version": dotnet_version,
        "pywin32_installed": bool(importlib.util.find_spec("win32com")),
        "progid_status": progid_status,
        "dir_candidates": dirs,
        "dll_exe_candidates": candidates,
    }

    write_report(args.out_md, data)
    print(
        {
            "out_md": str(args.out_md),
            "python_bits": data["python_bits"],
            "dotnet_version": data["dotnet_version"],
            "pywin32_installed": data["pywin32_installed"],
            "registered_progid_count": sum(1 for s in progid_status if s.registered),
            "activatable_progid_count": sum(1 for s in progid_status if s.activatable),
        }
    )


if __name__ == "__main__":
    main()
