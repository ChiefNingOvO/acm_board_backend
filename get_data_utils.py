import requests
import json
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ------------------------- 配置获取 --------------------------------------

# PTA API URLs
get_submit_url = os.getenv("PTA_SUBMIT_URL", "https://pintia.cn/api/problem-sets/1904101354045763584/submissions")
get_problem_url = os.getenv("PTA_PROBLEM_URL", "https://pintia.cn/api/problem-sets/1904101354045763584/problem-types")
get_rank_url = os.getenv("PTA_RANK_URL", "https://pintia.cn/api/problem-sets/1904101354045763584/common-rankings")

# PTA Headers 配置项
PTA_COOKIE = os.getenv("PTA_COOKIE", "")
PTA_X_LOLLIPOP = os.getenv("PTA_X_LOLLIPOP", "62f9808e932ddf27c102c045a623fe6e")

def get_base_headers(referer_path="submissions"):
    return {
        "accept": "application/json;charset=UTF-8",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN",
        "content-type": "application/json;charset=UTF-8",
        "cookie": PTA_COOKIE,
        "priority": "u=1, i",
        "referer": f"https://pintia.cn/problem-sets/1904101354045763584/{referer_path}",
        "sec-ch-ua": "\"Chromium\";v=\"146\", \"Not-A.Brand\";v=\"24\", \"Microsoft Edge\";v=\"146\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
        "x-lollipop": PTA_X_LOLLIPOP,
        "x-marshmallow": ""
    }

# ------------------------- 获取提交记录 --------------------------------------

get_submit_params = {
    "limit": 1000, # 多少条数据
    "filter": "{}"
}

def get_submit_headers():
    return get_base_headers("submissions")

# ------------------------- 获取问题 --------------------------------------

get_problem_params = {}

def get_problem_headers():
    return get_base_headers("submissions") # 沿用之前的 referer

# ------------------------- 获取排名 --------------------------------------

get_rank_params = {
    "page": 0,
    "limit": 100
}

def get_rank_headers():
    return get_base_headers("rankings")

def get_pintia_submissions():
    print("[get_pintia_submissions] 开始请求PTA提交记录接口...")
    try:
        response = requests.get(
            url=get_submit_url,
            params=get_submit_params,
            headers=get_submit_headers(),
            timeout=10,
            allow_redirects=True
        )


        response.raise_for_status()

        data = response.json()
        # 提交id（过滤重复数据），用户id，用户名，状态，问题id, 时间
        submissionId, userId, status, problemId, judgeTime = [], [], [], [], []
        for item in data["submissions"]:
            submissionId.append(item["id"])
            userId.append(item["userId"])
            status.append(item["status"])
            problemId.append(item["problemSetProblemId"])
            judgeTime.append(item["judgeAt"])

        cur_user_id, cur_user_name, cur_school_id = [], [], []
        student_users = data.get("studentUserById") or {}
        for student_key, student_info in student_users.items():
            if not student_info:
                continue
            student_number = student_info.get("studentNumber", "")
            name = student_info.get("name", "")
            student_id = student_info.get("id", "")
            cur_user_id.append(student_id)
            cur_user_name.append(name)
            cur_school_id.append(student_number)

        student_nick_id, student_real_id = [], []
        ids = data.get("examMemberByUserId") or {}
        for nick_id, nick_info in ids.items():
            if not nick_info:
                continue
            student_nick_id.append(nick_id)
            student_real_id.append(nick_info.get("studentUserId", ""))


        return submissionId, userId, status, problemId, judgeTime, cur_user_id, cur_user_name, cur_school_id, student_nick_id, student_real_id

    except requests.exceptions.Timeout:
        print("[get_pintia_submissions] 错误：请求超时，请检查网络或重试")
    except requests.exceptions.ConnectionError:
        print("[get_pintia_submissions] 错误：网络连接失败，请检查网络")
    except requests.exceptions.HTTPError as e:
        print(f"[get_pintia_submissions] 错误：HTTP请求失败，状态码 {e.response.status_code}")
        # 打印响应内容，便于排查问题（如未登录、权限不足）
        print(f"[get_pintia_submissions] 响应内容：{e.response.text}")
    except json.JSONDecodeError:
        print("[get_pintia_submissions] 错误：响应不是有效的JSON格式")
    except Exception as e:
        print(f"[get_pintia_submissions] 未知错误：{str(e)}")

    return None


def get_problem_types():
    """
    发起GET请求获取拼题啦题库的题目类型数据
    :return: dict/None  返回解析后的JSON数据（失败返回None）
    """
    print("[get_problem_types] 开始请求拼题啦题库类型数据接口...")
    try:
        # 发起GET请求（同步替换为新的变量名）
        response = requests.get(
            url=get_problem_url,
            params=get_problem_params,
            headers=get_problem_headers(),
            timeout=15,  # 超时时间15秒
            allow_redirects=True
        )

        # 检查HTTP状态码（非200则抛出异常）
        response.raise_for_status()

        # 解析JSON响应（转为Python字典）
        data = response.json()
        problemId, label = [], []
        for item in data["labels"]:
            problemId.append(item["id"])
            label.append(item["label"])
        return problemId, label

    # 捕获常见异常
    except requests.exceptions.Timeout:
        print("[get_problem_types] ❌ 错误：请求超时，请检查网络或重试")
    except requests.exceptions.ConnectionError:
        print("[get_problem_types] ❌ 错误：网络连接失败，请检查网络")
    except requests.exceptions.HTTPError as e:
        print(f"[get_problem_types] ❌ 错误：HTTP请求失败，状态码 {e.response.status_code}")
        print(f"[get_problem_types] ❌ 响应内容：{e.response.text}")
    except json.JSONDecodeError:
        print("[get_problem_types] ❌ 错误：响应不是有效的JSON格式")
    except Exception as e:
        print(f"[get_problem_types] ❌ 未知错误：{str(e)}")

    return None

def get_common_rankings():
    try:
        response = requests.get(
            url=get_rank_url,
            params=get_rank_params,
            headers=get_rank_headers(),
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        id_to_name = {}
        for item in data["studentUserById"]:
            name = data["studentUserById"][item]["name"]
            stu_id = data["studentUserById"][item]["id"]
            stu_number = data["studentUserById"][item]["studentNumber"]
            id_to_name.setdefault(stu_id, f"{name} {stu_number}")

        rank_to_name = {}
        for item in data["commonRankings"]:
            rank = item["rank"]
            stu_id = item["user"]["studentUserId"]
            stu_name = id_to_name.get(stu_id)
            rank_to_name.setdefault(rank, stu_name)

        return rank_to_name

    except Exception as e:
        print("❌ 请求失败：", str(e))
        return None
