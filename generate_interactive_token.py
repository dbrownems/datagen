"""Generate a Power BI access token interactively and save to .token file.

Run this script directly (not from subprocess) to authenticate interactively
and save the token for other scripts to use.

Usage:
    python generate_interactive_token.py          # one-shot
    python generate_interactive_token.py --loop   # refresh every 40 minutes
"""
import subprocess
import sys
import time

def refresh_token():
    result = subprocess.check_output(
        ["powershell.exe", "-NoProfile", "-Command",
         "Connect-PowerBIServiceAccount | Out-Null; "
         "(Get-PowerBIAccessToken -AsString).Replace('Bearer ','')"],
        text=True,
    ).strip()

    with open(".token", "w") as f:
        f.write(result)

    print(f"Token saved to .token ({len(result)} chars)")
    return result

print("Connecting to Power BI (may open browser for login)...")
refresh_token()

if "--loop" in sys.argv:
    interval = 40 * 60  # 40 minutes
    print(f"Refreshing every {interval // 60} minutes. Press Ctrl+C to stop.")
    while True:
        time.sleep(interval)
        try:
            refresh_token()
            print(f"  Refreshed at {time.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"  Refresh failed: {e}")
else:
    print("Token expires in ~60 minutes. Use --loop to auto-refresh.")
