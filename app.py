import sqlite3
import os
from fastapi import FastAPI, BackgroundTasks, HTTPException
from contextlib import asynccontextmanager
import uvicorn
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

from get_data_utils import get_pintia_submissions, get_problem_types, get_common_rankings
from clear_db import clear_database

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

DB_FILE = os.getenv("DB_FILE", "acm_board.db")

# 提交记录集合
submissionId_set = set()

# 学生通过题号集合，格式: {userId: set(problemId)}
student_passed_problems = {}

# 首刀记录，默认题号 A-L，-1 表示无人通过
first_blood = {chr(ord('A') + i): "-1" for i in range(12)}

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

def reset_runtime_state():
    global submissionId_set, student_passed_problems, first_blood, problem_label, user_info, id_info, message_queue, broadcast_queue

    submissionId_set = set()
    student_passed_problems = {}
    first_blood = {chr(ord('A') + i): "-1" for i in range(12)}
    problem_label = {}
    user_info = {}
    id_info = {}
    message_queue = []
    broadcast_queue = []

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    print("[init_db] 检查并初始化数据库表结构")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS submissions (submission_id TEXT PRIMARY KEY)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS student_passed_problems (user_id TEXT, status TEXT, problem_id TEXT, PRIMARY KEY (user_id, problem_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS first_blood (problem_id TEXT PRIMARY KEY, user_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS problem_label (problem_id TEXT PRIMARY KEY, label TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_info (user_id TEXT PRIMARY KEY, user_name TEXT, school_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS id_info (nick TEXT PRIMARY KEY, real TEXT)''')
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
    yield

# FastAPI 应用实例
app = FastAPI(title="ACM Show Submit API", lifespan=lifespan)

def init_data():
    """
    初始化所有数据：检查文件，若存在则读取历史数据加载到内存中。
    """
    print("[init_data] 开始执行初始化数据操作...")
    global submissionId_set, student_passed_problems, first_blood, problem_label, user_info, id_info

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

    # 3. 初始化并读取首刀记录
    try:
        cursor.execute("SELECT problem_id, user_id FROM first_blood")
        for row in cursor.fetchall():
            problem_id = row["problem_id"].strip()
            user_id = row["user_id"].strip()
            first_blood[problem_id] = user_id
    except Exception as e:
        print(f"[init_data] 读取首刀记录 DB 失败：{e}")

    # 4. 初始化题目与标签
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

    # 5. 初始化并读取用户信息
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

    # 6. 初始化映射文件
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
            handle_data(user_id, status, prob_id, judge_time)
            
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


def handle_data(user_id: str, status: str, problem_id: str, judge_time: str):
    """
    处理单条提交数据，根据状态和题目记录AC情况及首刀情况
    """
    print(f"[handle_data] 处理用户 {user_id} 提交的题目 {problem_id}，状态: {status}")
    if user_id not in student_passed_problems:
        student_passed_problems[user_id] = set()

    # 只有"通过"且"题目未在已通过列表中"时才进行记录
    if status == "ACCEPTED":
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
            handle_first_blood(user_id, problem_id)

    # 无论状态如何都发送通知
    send_message(user_id, problem_id, status, judge_time)


def handle_first_blood(user_id: str, problem_id: str):
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
    if first_blood.get(problem_id_label) == "-1":
        first_blood[problem_id_label] = user_id_str
        
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO first_blood (problem_id, user_id) VALUES (?, ?)", (problem_id_label, user_id_str))
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
            "problem_id": problem_id_label
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
        "name": user_name,
        "label": label,
        "status": status,
        "judge_time": judge_time
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


@app.get("/api/get_latest_submission")
def get_latest_submission():
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

        message_queue.clear()

        return current_messages

    except Exception as e:
        print(f"[get_latest_submission] 获取或处理提交记录时发生错误：{str(e)}")
        # 出错时返回空列表以符合前端期望的数据结构
        return []

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

@app.get("/api/send_message")
def send_message_api(content: str, duration: int):
    global broadcast_queue
    msg = {
        "type": "broadcast",
        "content": content,
        "duration": duration
    }
    broadcast_queue.append(msg)
    return {"status": "success", "message": "Message added to queue"}

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
    with open("admin.html", "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8090, reload=True)
