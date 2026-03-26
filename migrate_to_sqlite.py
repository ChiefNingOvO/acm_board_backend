import sqlite3
import csv
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_FILE = Path(os.environ["DB_FILE"])
if not DB_FILE.is_absolute():
    DB_FILE = BASE_DIR / DB_FILE
SUBMISSION_IDS_CSV = Path(os.environ["MIGRATION_SUBMISSION_IDS_CSV"])
STUDENT_PASSED_PROBLEMS_CSV = Path(os.environ["MIGRATION_STUDENT_PASSED_PROBLEMS_CSV"])
USER_PROBLEM_CSV = Path(os.environ["MIGRATION_USER_PROBLEM_CSV"])
PROBLEM_LABEL_CSV = Path(os.environ["MIGRATION_PROBLEM_LABEL_CSV"])
USER_INFO_CSV = Path(os.environ["MIGRATION_USER_INFO_CSV"])
ID_INFO_CSV = Path(os.environ["MIGRATION_ID_INFO_CSV"])

def migrate():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS submissions (submission_id TEXT PRIMARY KEY)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS student_passed_problems (user_id TEXT, status TEXT, problem_id TEXT, PRIMARY KEY (user_id, problem_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS first_blood (problem_id TEXT PRIMARY KEY, user_id TEXT, judge_time TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS problem_label (problem_id TEXT PRIMARY KEY, label TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_info (user_id TEXT PRIMARY KEY, user_name TEXT, school_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS id_info (nick TEXT PRIMARY KEY, real TEXT)''')

    if SUBMISSION_IDS_CSV.exists():
        with open(SUBMISSION_IDS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("INSERT OR IGNORE INTO submissions (submission_id) VALUES (?)", (row["submissionId"],))
    
    if STUDENT_PASSED_PROBLEMS_CSV.exists():
        with open(STUDENT_PASSED_PROBLEMS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("INSERT OR IGNORE INTO student_passed_problems (user_id, status, problem_id) VALUES (?, ?, ?)", (row["userId"], row["status"], row["problemId"]))

    if USER_PROBLEM_CSV.exists():
        with open(USER_PROBLEM_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    "INSERT OR IGNORE INTO first_blood (problem_id, user_id, judge_time) VALUES (?, ?, ?)",
                    (row["problemId"], row["userId"], row.get("judge_time")),
                )

    if PROBLEM_LABEL_CSV.exists():
        with open(PROBLEM_LABEL_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("INSERT OR IGNORE INTO problem_label (problem_id, label) VALUES (?, ?)", (row["problemId"], row["label"]))

    if USER_INFO_CSV.exists():
        with open(USER_INFO_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("INSERT OR IGNORE INTO user_info (user_id, user_name, school_id) VALUES (?, ?, ?)", (row["user_id"], row["user_name"], row["school_id"]))

    if ID_INFO_CSV.exists():
        with open(ID_INFO_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute("INSERT OR IGNORE INTO id_info (nick, real) VALUES (?, ?)", (row["nick"], row["real"]))

    conn.commit()
    conn.close()
    print("Migration complete!")

if __name__ == "__main__":
    migrate()
