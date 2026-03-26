import asyncio
import sqlite3
import os
import json
import time
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager, suppress
import uvicorn
from dotenv import load_dotenv
from kafka import KafkaProducer

# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
load_dotenv(ENV_FILE)

from get_data_utils import get_pintia_submissions, get_problem_types, get_common_rankings
from clear_db import clear_database

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

DB_FILE = Path(os.environ["DB_FILE"])
if not DB_FILE.is_absolute():
    DB_FILE = BASE_DIR / DB_FILE

APP_HOST = os.environ["APP_HOST"]
APP_PORT = int(os.environ["APP_PORT"])
APP_RELOAD = os.environ["APP_RELOAD"].lower() == "true"
APP_TITLE = os.environ["APP_TITLE"]
APP_SOURCE_NAME = os.environ["APP_SOURCE_NAME"]
ADMIN_HTML_FILE = os.environ["ADMIN_HTML_FILE"]
KAFKA_BOOTSTRAP_SERVERS = [
    server.strip()
    for server in os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
    if server.strip()
]
KAFKA_SUBMISSION_TOPIC = os.environ["KAFKA_SUBMISSION_TOPIC"]
KAFKA_BROADCAST_TOPIC = os.environ["KAFKA_BROADCAST_TOPIC"]
KAFKA_PRODUCER_ACKS = os.environ["KAFKA_PRODUCER_ACKS"]
KAFKA_PRODUCER_RETRIES = int(os.environ["KAFKA_PRODUCER_RETRIES"])
KAFKA_SEND_TIMEOUT_SECONDS = float(os.environ["KAFKA_SEND_TIMEOUT_SECONDS"])
SUBMISSION_POLL_INTERVAL_SECONDS = float(os.environ["SUBMISSION_POLL_INTERVAL_SECONDS"])
FIRST_BLOOD_PROBLEM_IDS = [
    item.strip()
    for item in os.environ["FIRST_BLOOD_PROBLEM_IDS"].split(",")
    if item.strip()
]
FIRST_BLOOD_EMPTY_VALUE = os.environ["FIRST_BLOOD_EMPTY_VALUE"]

# 提交记录集合
submissionId_set = set()

# 学生通过题号集合，格式: {userId: set(problemId)}
student_passed_problems = {}
accepted_submission_count = 0

# 首刀记录，默认题号 A-L，-1 表示无人通过
def build_empty_first_blood_record():
    return {
        "user_id": FIRST_BLOOD_EMPTY_VALUE,
        "judge_time": None,
    }


def build_default_first_blood():
    return {problem_id: build_empty_first_blood_record() for problem_id in FIRST_BLOOD_PROBLEM_IDS}


first_blood = build_default_first_blood()

# 题目与标签字典
problem_label = {}

# 用户信息字典，格式: {user_id: {"user_name": user_name, "school_id": school_id}}
user_info = {}

# 学号映射，格式: {nick_id: real_id}
id_info = {}

# 用于存储待发送给前端的消息列表
message_queue = []

# 用于存储待发送给前端的广播消息列表
broadcast_queue = []
submission_event_producer = None
submission_poller_task = None

def reset_runtime_state():
    global submissionId_set, student_passed_problems, accepted_submission_count, first_blood, problem_label, user_info, id_info, message_queue, broadcast_queue

    submissionId_set = set()
    student_passed_problems = {}
    accepted_submission_count = 0
    first_blood = build_default_first_blood()
    problem_label = {}
    user_info = {}
    id_info = {}
    message_queue = []
    broadcast_queue = []

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def get_submission_event_producer():
    global submission_event_producer

    if submission_event_producer is None:
        submission_event_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
            acks=KAFKA_PRODUCER_ACKS,
            retries=KAFKA_PRODUCER_RETRIES,
        )

    return submission_event_producer


def close_submission_event_producer():
    global submission_event_producer

    if submission_event_producer is not None:
        submission_event_producer.close()
        submission_event_producer = None


def publish_submission_events(messages: list[dict]):
    if not messages:
        return 0

    producer = get_submission_event_producer()

    for index, message in enumerate(messages):
        event_type = message.get("type", "message")
        event_key = message.get("event_key") or message.get("problem_id") or message.get("label") or f"event-{index}"
        event_payload = {
            "event_type": event_type,
            "source": APP_SOURCE_NAME,
            "payload": message,
        }
        producer.send(
            KAFKA_SUBMISSION_TOPIC,
            key=str(event_key),
            value=event_payload,
        ).get(timeout=KAFKA_SEND_TIMEOUT_SECONDS)

    producer.flush()
    return len(messages)


def sync_latest_submissions_to_kafka():
    global message_queue

    print("[submission_poller] 正在拉取最新提交并发送到 Kafka...")
    res = get_pintia_submissions()

    if res is not None:
        submission_ids, user_ids, statuses, problem_ids, judge_times, cur_user_id, cur_user_name, cur_school_id, student_nick_id, student_real_id = res

        save_user_info(cur_user_id, cur_user_name, cur_school_id)
        save_student_info(student_nick_id, student_real_id)

        if submission_ids:
            filter_data(submission_ids, user_ids, statuses, problem_ids, judge_times)

    current_messages = list(message_queue)
    published_count = publish_submission_events(current_messages)
    message_queue.clear()
    print(f"[submission_poller] 本轮完成，发送到 Kafka 的事件数: {published_count}")
    return published_count


def parse_judge_time(judge_time: str | None):
    if not judge_time:
        return None

    try:
        return datetime.fromisoformat(judge_time.replace("Z", "+00:00"))
    except ValueError:
        return None


def should_update_first_blood(saved_record: dict, current_judge_time: str):
    if not isinstance(saved_record, dict):
        saved_record = {
            "user_id": saved_record,
            "judge_time": None,
        }

    if saved_record.get("user_id") == FIRST_BLOOD_EMPTY_VALUE:
        return True

    saved_judge_time = saved_record.get("judge_time")
    if not saved_judge_time:
        return False

    current_dt = parse_judge_time(current_judge_time)
    saved_dt = parse_judge_time(saved_judge_time)

    if current_dt is not None and saved_dt is not None:
        return current_dt < saved_dt

    return current_judge_time < saved_judge_time


async def submission_poller_loop():
    while True:
        try:
            await asyncio.to_thread(sync_latest_submissions_to_kafka)
        except Exception as e:
            print(f"[submission_poller] 拉取或发送失败: {e}")

        await asyncio.sleep(SUBMISSION_POLL_INTERVAL_SECONDS)

def init_db():
    print("[init_db] 检查并初始化数据库表结构")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS submissions (submission_id TEXT PRIMARY KEY)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS student_passed_problems (user_id TEXT, status TEXT, problem_id TEXT, PRIMARY KEY (user_id, problem_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS accepted_submissions (submission_id TEXT PRIMARY KEY)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS first_blood (problem_id TEXT PRIMARY KEY, user_id TEXT, judge_time TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS problem_label (problem_id TEXT PRIMARY KEY, label TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_info (user_id TEXT PRIMARY KEY, user_name TEXT, school_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS id_info (nick TEXT PRIMARY KEY, real TEXT)''')
    first_blood_columns = {row[1] for row in cursor.execute("PRAGMA table_info(first_blood)").fetchall()}
    if "judge_time" not in first_blood_columns:
        cursor.execute("ALTER TABLE first_blood ADD COLUMN judge_time TEXT")
    conn.commit()
    conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理：启动时初始化数据
    """
    print("[lifespan] 正在初始化数据...")
    init_data()
    print("[lifespan] 初始化完成！")
    global submission_poller_task
    submission_poller_task = asyncio.create_task(submission_poller_loop())

    try:
        yield
    finally:
        if submission_poller_task is not None:
            submission_poller_task.cancel()
            with suppress(asyncio.CancelledError):
                await submission_poller_task
            submission_poller_task = None

        close_submission_event_producer()

# FastAPI 应用实例
app = FastAPI(title=APP_TITLE, lifespan=lifespan)

def init_data():
    """
    初始化所有数据：检查文件，若存在则读取历史数据加载到内存中。
    """
    print("[init_data] 开始执行初始化数据操作...")
    global submissionId_set, student_passed_problems, accepted_submission_count, first_blood, problem_label, user_info, id_info

    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. 初始化并读取提交记录
    try:
        cursor.execute("SELECT submission_id FROM submissions")
        for row in cursor.fetchall():
            submissionId_set.add(row["submission_id"].strip())
    except Exception as e:
        print(f"[init_data] 读取提交记录 DB 失败：{e}")

    # 2. 初始化并读取学生AC记录
    try:
        cursor.execute("SELECT user_id, status, problem_id FROM student_passed_problems")
        for row in cursor.fetchall():
            user_id = row["user_id"].strip()
            status = row["status"].strip()
            problem_id = row["problem_id"].strip()
            
            if user_id not in student_passed_problems:
                student_passed_problems[user_id] = set()
            
            if status == "ACCEPTED":
                student_passed_problems[user_id].add(problem_id)
    except Exception as e:
        print(f"[init_data] 读取学生AC记录 DB 失败：{e}")

    # 3. 初始化并读取所有AC提交次数
    try:
        cursor.execute("SELECT COUNT(*) AS accepted_count FROM accepted_submissions")
        row = cursor.fetchone()
        accepted_submission_count = int(row["accepted_count"]) if row else 0
    except Exception as e:
        print(f"[init_data] 读取 accepted_submissions 失败：{e}")

    # 4. 初始化并读取首刀记录
    try:
        cursor.execute("SELECT problem_id, user_id, judge_time FROM first_blood")
        for row in cursor.fetchall():
            problem_id = row["problem_id"].strip()
            user_id = row["user_id"].strip()
            judge_time = row["judge_time"]
            first_blood[problem_id] = {
                "user_id": user_id,
                "judge_time": judge_time.strip() if isinstance(judge_time, str) else judge_time,
            }
    except Exception as e:
        print(f"[init_data] 读取首刀记录 DB 失败：{e}")

    # 5. 初始化题目与标签
    try:
        cursor.execute("SELECT problem_id, label FROM problem_label")
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                problem_label[row["problem_id"].strip()] = row["label"].strip()
        else:
            print("[init_data] 数据库未找到题目标签，正在从接口获取数据...")
            res = get_problem_types()
            if res is not None:
                problem_ids, labels = res
                try:
                    for p_id, lbl in zip(problem_ids, labels):
                        problem_label[p_id] = lbl
                        cursor.execute("INSERT OR REPLACE INTO problem_label (problem_id, label) VALUES (?, ?)", (p_id, lbl))
                    conn.commit()
                    print("[init_data] 题目标签 DB 创建并写入成功。")
                except Exception as e:
                    print(f"[init_data] 创建题目标签 DB 失败：{e}")
            else:
                print("[init_data] 获取题目标签失败。")
    except Exception as e:
        print(f"[init_data] 读取题目标签记录 DB 失败：{e}")

    # 6. 初始化并读取用户信息
    try:
        cursor.execute("SELECT user_id, user_name, school_id FROM user_info")
        for row in cursor.fetchall():
            user_id = row["user_id"].strip()
            user_info[user_id] = {
                "user_name": row["user_name"].strip(),
                "school_id": row["school_id"].strip()
            }
    except Exception as e:
        print(f"[init_data] 读取用户信息记录 DB 失败：{e}")

    # 7. 初始化映射文件
    try:
        cursor.execute("SELECT nick, real FROM id_info")
        for row in cursor.fetchall():
            id_info[row["nick"].strip()] = row["real"].strip()
    except Exception as e:
        print(f"[init_data] 读取用户信息 DB 失败：{e}")

    conn.close()

def filter_data(submission_ids: list, user_ids: list, statuses: list, problem_ids: list, judge_times: list):
    """
    过滤重复提交数据，通过 submissionId 去重，并批量处理新数据
    """
    print(f"[filter_data] 收到 {len(submission_ids)} 条数据，开始过滤...")
    new_submissions = []
    
    for sub_id, user_id, status, prob_id, judge_time in zip(submission_ids, user_ids, statuses, problem_ids, judge_times):
        if sub_id not in submissionId_set:
            submissionId_set.add(sub_id)
            new_submissions.append((sub_id,))
            
            # 处理业务逻辑
            handle_data(sub_id, user_id, status, prob_id, judge_time)
            
    print(f"[filter_data] 新提交记录数: {len(new_submissions)}")

    if new_submissions:
        conn = get_db_connection()
        try:
            conn.executemany("INSERT OR IGNORE INTO submissions (submission_id) VALUES (?)", new_submissions)
            conn.commit()
        except Exception as e:
            print(f"[filter_data] 写入 submissions 失败: {e}")
        finally:
            conn.close()


def handle_data(submission_id: str, user_id: str, status: str, problem_id: str, judge_time: str):
    """
    处理单条提交数据，根据状态和题目记录AC情况及首刀情况
    """
    global accepted_submission_count

    print(f"[handle_data] 处理提交 {submission_id}，用户 {user_id} 的题目 {problem_id}，状态: {status}")
    if user_id not in student_passed_problems:
        student_passed_problems[user_id] = set()

    # 只有"通过"且"题目未在已通过列表中"时才进行记录
    if status == "ACCEPTED":
        conn = get_db_connection()
        try:
            cursor = conn.execute("INSERT OR IGNORE INTO accepted_submissions (submission_id) VALUES (?)", (submission_id,))
            conn.commit()
            if cursor.rowcount == 1:
                accepted_submission_count += 1
        except Exception as e:
            print(f"[handle_data] 写入 accepted_submissions 失败: {e}")
        finally:
            conn.close()

        if problem_id not in student_passed_problems[user_id]:
            student_passed_problems[user_id].add(problem_id)
            
            conn = get_db_connection()
            try:
                conn.execute("INSERT OR REPLACE INTO student_passed_problems (user_id, status, problem_id) VALUES (?, ?, ?)", (user_id, status, problem_id))
                conn.commit()
            except Exception as e:
                print(f"[handle_data] 写入 student_passed_problems 失败: {e}")
            finally:
                conn.close()
                
            # 处理首刀逻辑
            handle_first_blood(user_id, problem_id, judge_time)

    # 无论状态如何都发送通知
    send_message(user_id, problem_id, status, judge_time)


def handle_first_blood(user_id: str, problem_id: str, judge_time: str):
    """
    处理首刀（First Blood）逻辑
    """
    print(f"[handle_first_blood] 检查用户 {user_id} 对题目 {problem_id} 的首刀情况")
    global first_blood
    global problem_label

    problem_id_label = problem_label.get(problem_id, problem_id)
    mapped_user_id = id_info.get(user_id, user_id)
    user_data = user_info.get(mapped_user_id, {})
    user_name = user_data.get("user_name", "未知用户")
    school_id = user_data.get("school_id", "未知学校")
    user_id_str = f"{user_name} {school_id}"

    # 逻辑修复：如果该题目前没有人 AC（值为 "-1"），则当前用户夺得首刀
    saved_record = first_blood.get(problem_id_label, build_empty_first_blood_record())

    if should_update_first_blood(saved_record, judge_time):
        first_blood[problem_id_label] = {
            "user_id": user_id_str,
            "judge_time": judge_time,
        }
        
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO first_blood (problem_id, user_id, judge_time) VALUES (?, ?, ?)",
                (problem_id_label, user_id_str, judge_time),
            )
            conn.commit()
        except Exception as e:
            print(f"[handle_first_blood] 写入 first_blood 失败: {e}")
        finally:
            conn.close()
            
        msg = f"[handle_first_blood] 🎉 恭喜用户 {user_id_str} 获得题目 {problem_id_label} 的首刀！"
        print(msg)
        message_queue.append({
            "type": "first_blood",
            "message": msg,
            "user_id": user_id_str,
            "problem_id": problem_id_label,
            "judge_time": judge_time,
            "event_key": f"first_blood:{problem_id_label}"
        })


def send_message(user_id: str, problem_id: str, status: str, judge_time: str):
    """
    发送消息，通知用户提交题目的状态，并将消息保存供前端获取
    """
    print(f"[send_message] 准备发送用户 {user_id} 提交 {problem_id} ({status}) 的通知")
    global id_info, user_info, problem_label
    
    # 获取用户的姓名，如果未找到则使用默认值
    mapped_user_id = id_info.get(user_id, user_id)
    user_name = user_info.get(mapped_user_id, {}).get("user_name", "未知用户")
    # 获取题目标签（如 "A", "B"），如果未找到则使用原 problem_id
    label = problem_label.get(problem_id, problem_id)
    
    status_msg = "通过" if status == "ACCEPTED" else "未通过"
    msg = f"[send_message] 🔔 通知：用户 {user_name} 提交了题目 {label}，状态是 {status_msg} ({status})"
    print(msg)
    
    # 按前端要求，只保存包含 name, label, status 三个 key 的数据
    message_queue.append({
        "type": "message",
        "user_id": mapped_user_id,
        "problem_id": problem_id,
        "name": user_name,
        "label": label,
        "status": status,
        "judge_time": judge_time,
        "event_key": f"submission:{mapped_user_id}:{problem_id}:{status}:{judge_time}"
    })


def save_user_info(cur_user_id: list, cur_user_name: list, cur_school_id: list):
    """
    保存用户信息，将未记录的用户写入内存及 DB
    """
    global user_info
    print(f"[save_user_info] 检查 {len(cur_user_id)} 名用户信息...")
    new_users = []
    
    for u_id, u_name, s_id in zip(cur_user_id, cur_user_name, cur_school_id):
        if u_id not in user_info:
            user_info[u_id] = {
                "user_name": u_name,
                "school_id": s_id
            }
            new_users.append((u_id, u_name, s_id))
            
    if new_users:
        conn = get_db_connection()
        try:
            conn.executemany("INSERT OR REPLACE INTO user_info (user_id, user_name, school_id) VALUES (?, ?, ?)", new_users)
            conn.commit()
        except Exception as e:
            print(f"[save_user_info] 写入用户信息到 DB 失败：{e}")
        finally:
            conn.close()


def save_student_info(student_nick_id, student_real_id):
    """
    保存存映射信息，将未记录的用户写入内存及 DB
    """
    global id_info
    new_info = []

    for nick, real in zip(student_nick_id, student_real_id):
        if nick not in id_info:
            id_info[nick] = real
            new_info.append((nick, real))

    if new_info:
        conn = get_db_connection()
        try:
            conn.executemany("INSERT OR REPLACE INTO id_info (nick, real) VALUES (?, ?)", new_info)
            conn.commit()
        except Exception as e:
            print(f"[save_student_info] 写入映射信息到 DB 失败：{e}")
        finally:
            conn.close()


def get_latest_submission_once():
    """
    提供给前端的统一接口：
    调用时会主动拉取一次拼题啦最新数据进行处理。
    如果有产生新的提交消息，则返回这些消息（包含 name, label, status）；
    如果没有新消息，则返回空数组。
    """
    print("[get_latest_submission] 前端请求拉取最新提交数据...")
    global message_queue
    
    try:
        res = get_pintia_submissions()

        if res is not None:
            submission_ids, user_ids, statuses, problem_ids, judge_times, cur_user_id, cur_user_name, cur_school_id, student_nick_id, student_real_id = res

            save_user_info(cur_user_id, cur_user_name, cur_school_id)
            save_student_info(student_nick_id, student_real_id)

            if submission_ids:
                filter_data(submission_ids, user_ids, statuses, problem_ids, judge_times)

        current_messages = list(message_queue)
        published_count = publish_submission_events(current_messages)
        message_queue.clear()

        return {
            "status": "success",
            "published_count": published_count,
            "topic": KAFKA_SUBMISSION_TOPIC,
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        }

    except Exception as e:
        print(f"[get_latest_submission] 获取或处理提交记录时发生错误：{str(e)}")
        # 出错时返回空列表以符合前端期望的数据结构
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/get_rank_list")
def get_rank_list():
    rank_to_name = get_common_rankings()
    rank_list = []
    if rank_to_name:
        for rank_num, name in rank_to_name.items():
            rank_list.append({
                "rank": rank_num,
                "name": name
            })
    return rank_list


@app.get("/api/get_first_blood")
def get_first_blood_list():
    result = []

    for problem_id in sorted(first_blood.keys()):
        record = first_blood[problem_id]
        result.append({
            "problem_id": problem_id,
            "user_id": record["user_id"],
            "judge_time": record["judge_time"],
        })

    return result


@app.get("/api/get_submission_stats")
def get_submission_stats():
    return {
        "total_submissions": len(submissionId_set),
        "accepted_submissions": accepted_submission_count,
    }

@app.get("/api/send_message")
def send_message_api(content: str, duration: int):
    msg = {
        "type": "broadcast",
        "content": content,
        "duration": duration,
        "created_at": int(time.time() * 1000),
    }
    msg["event_key"] = f"broadcast:{msg['created_at']}"

    producer = get_submission_event_producer()
    producer.send(
        KAFKA_BROADCAST_TOPIC,
        key=msg["event_key"],
        value={
            "event_type": "broadcast",
            "source": APP_SOURCE_NAME,
            "payload": msg,
        },
    ).get(timeout=KAFKA_SEND_TIMEOUT_SECONDS)
    producer.flush()

    return {
        "status": "success",
        "topic": KAFKA_BROADCAST_TOPIC,
        "event_key": msg["event_key"],
    }

@app.get("/api/get_messages")
def get_messages():
    global broadcast_queue
    messages = list(broadcast_queue)
    broadcast_queue.clear()
    return messages

@app.post("/api/clear_db")
def clear_db_api():
    try:
        clear_database(DB_FILE)
        reset_runtime_state()
        init_data()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    with open(BASE_DIR / ADMIN_HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    uvicorn.run("app:app", host=APP_HOST, port=APP_PORT, reload=APP_RELOAD)
