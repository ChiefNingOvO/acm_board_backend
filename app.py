import csv
import os
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import uvicorn

from get_data_utils import get_pintia_submissions, get_problem_types

# 提交记录持久化文件
CSV_FILE_NAME_SUBMISSIONS = "submissionIds.csv"
CORE_COLUMNS_SUBMISSIONS = ["submissionId"]

# 学生AC持久化文件
CSV_FILE_NAME_STUDENT_PASSED_PROBLEMS = "student_passed_problems.csv"
CORE_COLUMNS = ["userId", "status", "problemId"]

# 首刀配置持久化文件
CSV_FILE_NAME_FIRST_BLOOD = "user_problem.csv"
CORE_COLUMNS_FIRST_BLOOD = ["problemId", "userId"]

# 题目与标签持久化文件
CSV_FILE_NAME_PROBLEM_LABEL = "problem_label.csv"
CORE_COLUMNS_PROBLEM_LABEL = ["problemId", "label"]

# 用户信息持久化文件
CSV_FILE_NAME_USER_INFO = "user_info.csv"
CORE_COLUMNS_USER_INFO = ["user_id", "user_name", "school_id"]

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

# 用于存储待发送给前端的消息列表
message_queue = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理：启动时初始化数据
    """
    print("正在初始化数据...")
    init_data()
    print("初始化完成！")
    yield
    # 这里可以添加清理代码，如关闭数据库连接等

# FastAPI 应用实例
app = FastAPI(title="ACM Show Submit API", lifespan=lifespan)

def init_csv_file(filename: str, fieldnames: list):
    """
    通用 CSV 初始化函数：
    检查文件是否存在，若不存在则创建并写入表头。
    """
    if not os.path.exists(filename):
        with open(filename, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def init_data():
    """
    初始化所有数据：检查文件，若存在则读取历史数据加载到内存中。
    """
    global submissionId_set, student_passed_problems, first_blood, problem_label, user_info

    # 1. 初始化并读取提交记录
    init_csv_file(CSV_FILE_NAME_SUBMISSIONS, CORE_COLUMNS_SUBMISSIONS)
    try:
        with open(CSV_FILE_NAME_SUBMISSIONS, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                submissionId_set.add(row["submissionId"].strip())
    except Exception as e:
        print(f"读取提交记录 CSV 文件失败：{e}")

    # 2. 初始化并读取学生AC记录
    init_csv_file(CSV_FILE_NAME_STUDENT_PASSED_PROBLEMS, CORE_COLUMNS)
    try:
        with open(CSV_FILE_NAME_STUDENT_PASSED_PROBLEMS, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            missing_cols = [col for col in CORE_COLUMNS if col not in reader.fieldnames]
            if missing_cols:
                raise ValueError(f"CSV缺少必要列：{missing_cols}，请检查文件格式")

            for row in reader:
                user_id = row["userId"].strip()
                status = row["status"].strip()
                problem_id = row["problemId"].strip()
                
                if user_id not in student_passed_problems:
                    student_passed_problems[user_id] = set()
                
                if status == "ACCEPT":
                    student_passed_problems[user_id].add(problem_id)
    except Exception as e:
        print(f"读取学生AC记录失败：{e}")

    # 3. 初始化并读取首刀记录
    init_csv_file(CSV_FILE_NAME_FIRST_BLOOD, CORE_COLUMNS_FIRST_BLOOD)
    try:
        with open(CSV_FILE_NAME_FIRST_BLOOD, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            missing_cols = [col for col in CORE_COLUMNS_FIRST_BLOOD if col not in reader.fieldnames]
            if missing_cols:
                raise ValueError(f"CSV缺少必要列：{missing_cols}，仅支持{CORE_COLUMNS_FIRST_BLOOD}")
            
            for row in reader:
                problem_id = row["problemId"].strip()
                user_id = row["userId"].strip()
                first_blood[problem_id] = user_id
    except Exception as e:
        print(f"读取首刀记录失败：{e}")

    # 4. 初始化题目与标签
    if os.path.exists(CSV_FILE_NAME_PROBLEM_LABEL):
        try:
            with open(CSV_FILE_NAME_PROBLEM_LABEL, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    problem_label[row["problemId"].strip()] = row["label"].strip()
        except Exception as e:
            print(f"读取题目标签记录失败：{e}")
    else:
        print("未找到题目标签文件，正在从接口获取数据...")
        res = get_problem_types()
        if res is not None:
            problem_ids, labels = res
            try:
                with open(CSV_FILE_NAME_PROBLEM_LABEL, mode="w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS_PROBLEM_LABEL)
                    writer.writeheader()
                    for p_id, lbl in zip(problem_ids, labels):
                        problem_label[p_id] = lbl
                        writer.writerow({"problemId": p_id, "label": lbl})
                print("题目标签文件创建并写入成功。")
            except Exception as e:
                print(f"创建题目标签文件失败：{e}")
        else:
            print("获取题目标签失败。")

    # 5. 初始化并读取用户信息
    init_csv_file(CSV_FILE_NAME_USER_INFO, CORE_COLUMNS_USER_INFO)
    try:
        with open(CSV_FILE_NAME_USER_INFO, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            missing_cols = [col for col in CORE_COLUMNS_USER_INFO if col not in reader.fieldnames]
            if missing_cols:
                raise ValueError(f"CSV缺少必要列：{missing_cols}，请检查文件格式")
                
            for row in reader:
                user_id = row["user_id"].strip()
                user_info[user_id] = {
                    "user_name": row["user_name"].strip(),
                    "school_id": row["school_id"].strip()
                }
    except Exception as e:
        print(f"读取用户信息记录失败：{e}")


def filter_data(submission_ids: list, user_ids: list, statuses: list, problem_ids: list):
    """
    过滤重复提交数据，通过 submissionId 去重，并批量处理新数据
    """
    new_submissions = []
    
    # 使用 zip 将列表打包遍历，避免使用容易出错的 idx 索引
    for sub_id, user_id, status, prob_id in zip(submission_ids, user_ids, statuses, problem_ids):
        if sub_id not in submissionId_set:
            submissionId_set.add(sub_id)
            new_submissions.append({"submissionId": sub_id})
            
            # 处理业务逻辑
            handle_data(user_id, status, prob_id)
            
    # 批量将新的提交记录持久化，减少频繁的 I/O 开销
    if new_submissions:
        with open(CSV_FILE_NAME_SUBMISSIONS, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS_SUBMISSIONS)
            writer.writerows(new_submissions)


def handle_data(user_id: str, status: str, problem_id: str):
    """
    处理单条提交数据，根据状态和题目记录AC情况及首刀情况
    """
    if user_id not in student_passed_problems:
        student_passed_problems[user_id] = set()

    # 只有"通过"且"题目未在已通过列表中"时才进行记录
    if status == "ACCEPTED":
        if problem_id not in student_passed_problems[user_id]:
            student_passed_problems[user_id].add(problem_id)
            
            new_record = {
                "userId": user_id,
                "status": status,
                "problemId": problem_id
            }

            with open(CSV_FILE_NAME_STUDENT_PASSED_PROBLEMS, mode="a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS)
                writer.writerow(new_record)
                
            # 处理首刀逻辑（原代码中写了函数但并未调用，在此处补充调用）
            handle_first_blood(user_id, problem_id)

    # 无论状态如何都发送通知
    send_message(user_id, problem_id, status)


def handle_first_blood(user_id: str, problem_id: str):
    """
    处理首刀（First Blood）逻辑
    """
    global first_blood
    global problem_label
    problem_id = problem_label.get(problem_id)
    user_id = user_info.get(user_id).get("user_name") + " " + user_info.get(user_id).get("school_id")

    # 逻辑修复：如果该题目前没有人 AC（值为 "-1"），则当前用户夺得首刀
    if first_blood.get(problem_id) == "-1":
        first_blood[problem_id] = user_id
        
        new_record = {
            "problemId": problem_id,
            "userId": user_id
        }
        
        with open(CSV_FILE_NAME_FIRST_BLOOD, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS_FIRST_BLOOD)
            writer.writerow(new_record)
            
        msg = f"🎉 恭喜用户 {user_id} 获得题目 {problem_id} 的首刀！"
        print(msg)
        message_queue.append({
            "type": "first_blood",
            "message": msg,
            "user_id": user_id,
            "problem_id": problem_id
        })


def send_message(user_id: str, problem_id: str, status: str):
    """
    发送消息，通知用户提交题目的状态，并将消息保存供前端获取
    """
    global user_info, problem_label
    
    # 获取用户的姓名，如果未找到则使用默认值
    user_name = user_info.get(user_id, {}).get("user_name", "未知用户")
    
    # 获取题目标签（如 "A", "B"），如果未找到则使用原 problem_id
    label = problem_label.get(problem_id, problem_id)
    
    status_msg = "通过" if status == "ACCEPTED" else "未通过"
    msg = f"🔔 通知：用户 {user_name} 提交了题目 {label}，状态是 {status_msg} ({status})"
    print(msg)
    
    # 按前端要求，只保存包含 name, label, status 三个 key 的数据
    message_queue.append({
        "type": "message",
        "name": user_name,
        "label": label,
        "status": status
    })


def save_user_info(cur_user_id: list, cur_user_name: list, cur_school_id: list):
    """
    保存用户信息，将未记录的用户写入内存及 CSV 文件
    """
    new_users = []
    
    for u_id, u_name, s_id in zip(cur_user_id, cur_user_name, cur_school_id):
        # 如果用户不在字典中，或者信息不完整（根据业务逻辑此处做简单存在性判断）
        if u_id not in user_info:
            # 写入内存
            user_info[u_id] = {
                "user_name": u_name,
                "school_id": s_id
            }
            # 记录新增数据
            new_users.append({
                "user_id": u_id,
                "user_name": u_name,
                "school_id": s_id
            })
            
    # 如果有新增用户，批量追加写入 CSV
    if new_users:
        try:
            with open(CSV_FILE_NAME_USER_INFO, mode="a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS_USER_INFO)
                writer.writerows(new_users)
        except Exception as e:
            print(f"写入用户信息到 CSV 失败：{e}")


@app.get("/api/get_latest_submission")
def get_latest_submission():
    """
    提供给前端的统一接口：
    调用时会主动拉取一次拼题啦最新数据进行处理。
    如果有产生新的提交消息，则返回这些消息（包含 name, label, status）；
    如果没有新消息，则返回空数组。
    """
    global message_queue
    
    try:
        res = get_pintia_submissions()

        if res is not None:
            submission_ids, user_ids, statuses, problem_ids, cur_user_id, cur_user_name, cur_school_id = res

            save_user_info(cur_user_id, cur_user_name, cur_school_id)

            if submission_ids:
                filter_data(submission_ids, user_ids, statuses, problem_ids)

        current_messages = list(message_queue)

        message_queue.clear()

        return current_messages

    except Exception as e:
        print(f"获取或处理提交记录时发生错误：{str(e)}")
        # 出错时返回空列表以符合前端期望的数据结构
        return []


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8090, reload=True)
