"""Download a small replay batch from the public NASA July 1995 access log."""

import gzip
import subprocess
import tempfile
from pathlib import Path
from urllib.request import urlopen


URL = "https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
RECORDS = 1000
DESTINATION = "/tmp/nasa-input"


def main():
    subprocess.run(["docker", "compose", "exec", "-T", "nifi", "mkdir", "-p", DESTINATION], check=True)
    with tempfile.TemporaryDirectory() as directory:
        sample = Path(directory) / "nasa-july-1995-sample.log"
        with urlopen(URL, timeout=60) as response, gzip.GzipFile(fileobj=response) as archive, sample.open("wb") as output:
            for _ in range(RECORDS):
                line = archive.readline()
                if not line:
                    break
                output.write(line)
        container = subprocess.check_output(["docker", "compose", "ps", "-q", "nifi"], text=True).strip()
        if not container:
            raise RuntimeError("NiFi is not running")
        subprocess.run(["docker", "cp", str(sample), f"{container}:{DESTINATION}/"], check=True)
    print(f"Copied up to {RECORDS} NASA log records to NiFi's {DESTINATION} directory.")


if __name__ == "__main__":
    main()
