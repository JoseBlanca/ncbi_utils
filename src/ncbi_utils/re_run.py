import time
import subprocess

while True:
    cmd = ["uv", "run", "query_sra.py"]
    process = subprocess.run(cmd)
    if not process.returncode:
        break
    time.sleep(2)
