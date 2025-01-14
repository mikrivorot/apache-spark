import os
import random
import time
from datetime import datetime
from typing import List

# Possible HTTP methods and URLs
METHODS_AND_URLS: List[str] = [
    "GET /books",
    "POST /books",
    "GET /books/xxx",
    "POST /admin",
]

# Possible response statuses
RESPONSE_STATUSES: List[int] = [200, 201, 400, 500]

def generate_random_ip() -> str:
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

def generate_fake_log() -> str:
    ip_address: str = generate_random_ip()
    timestamp: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")  # Current timestamp
    method_and_url: str = random.choice(METHODS_AND_URLS)
    response_status: int = random.choice(RESPONSE_STATUSES)
    content_size: int = random.randint(100, 5000)  # Random content size in bytes

    # <random IP address> [<current timestamp>] "<method name url>" <response status> <content size>
    log_line: str = f'{ip_address} [{timestamp}] "{method_and_url}" {response_status} {content_size}'
    return log_line

def write_logs_to_file() -> None:
    if not os.path.exists("logs"):
        os.makedirs("logs")

    filename: str = datetime.now().strftime("./streaming/logs/logs_%Y-%m-%dT%H-%M-%S%z.txt")

    fake_logs: List[str] = [generate_fake_log() for _ in range(10)]

    with open(filename, "w") as file:
        for log in fake_logs:
            file.write(log + "\n")
    print(f"Created log file: {filename}")

def main() -> None:
    print("Starting fake log generator")
    try:
        while True:
            write_logs_to_file()
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping fake log generator.")

if __name__ == "__main__":
    main()
