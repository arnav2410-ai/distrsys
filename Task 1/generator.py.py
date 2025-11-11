# utils/generate_logs.py
import random
import datetime

LOG_LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG']
MESSAGES = [
    'Connection established',
    'Request processed',
    'Database query executed',
    'Cache miss',
    'Authentication failed',
    'Disk read failed',
    'Memory allocation error',
    'Network timeout'
]

def generate_log_file(filename, num_lines=1000):
    with open(filename, 'w') as f:
        for i in range(num_lines):
            timestamp = datetime.datetime.now().isoformat()
            level = random.choice(LOG_LEVELS)
            message = random.choice(MESSAGES)
            f.write(f"[{timestamp}] [{level}] {message}\n")

# Generate multiple log files
for i in range(1, 11):
    generate_log_file(r"C:\Users\kinga\OneDrive\Documents\Task 1\node1.log", num_lines=5000)