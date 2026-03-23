import requests
import json

# ------------------------- 获取提交记录 --------------------------------------

get_submit_url = "https://pintia.cn/api/problem-sets/1904101354045763584/submissions"
get_submit_params = {
    "limit": 1000, # 多少条数据
    "filter": "{}"
}

get_submit_headers = {
    "accept": "application/json;charset=UTF-8",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN",
    "content-type": "application/json;charset=UTF-8",
    # ！！！替换成你自己的Cookie（关键，否则会返回未登录）！！！
    "cookie": "PTASession=c5d0d463-fb36-4912-862a-ad9127aaf3d9; _bl_uid=3vm26mCpx3U1e5v3RazLopLzpy1h; _ga=GA1.1.1723439565.1773899346; _ga_ZHCNP8KECW=GS2.1.s1774182487$o6$g1$t1774182579$j28$l0$h0; JSESSIONID=29D3FF4BDAE76BEF1AAE5F64BC1C8E11",
    "priority": "u=1, i",
    "referer": "https://pintia.cn/problem-sets/1904101354045763584/submissions",
    "sec-ch-ua": "\"Chromium\";v=\"146\", \"Not-A.Brand\";v=\"24\", \"Microsoft Edge\";v=\"146\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    "x-lollipop": "62f9808e932ddf27c102c045a623fe6e",
    "x-marshmallow": ""
}

# ------------------------- 获取问题 --------------------------------------

get_problem_url = "https://pintia.cn/api/problem-sets/1904101354045763584/problem-types"
get_problem_params = {}
get_problem_headers = {
    "accept": "application/json;charset=UTF-8",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN",
    "content-type": "application/json;charset=UTF-8",
    # ！！！替换为你自己的有效Cookie！！！
    "cookie": "PTASession=c5d0d463-fb36-4912-862a-ad9127aaf3d9; _bl_uid=3vm26mCpx3U1e5v3RazLopLzpy1h; _ga=GA1.1.1723439565.1773899346; _ga_ZHCNP8KECW=GS2.1.s1774189469$o7$g0$t1774189469$j60$l0$h0; JSESSIONID=677F9FDA28A283AEE74DBE04CBBF401B",
    "priority": "u=1, i",
    "referer": "https://pintia.cn/problem-sets/1904101354045763584/submissions",
    "sec-ch-ua": "\"Chromium\";v=\"146\", \"Not-A.Brand\";v=\"24\", \"Microsoft Edge\";v=\"146\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    "x-lollipop": "62f9808e932ddf27c102c045a623fe6e",
    "x-marshmallow": ""
}

def get_pintia_submissions():
    try:
        response = requests.get(
            url=get_submit_url,
            params=get_submit_params,
            headers=get_submit_headers,
            timeout=10,
            allow_redirects=True
        )

        response.raise_for_status()

        data = response.json()
        # 提交id（过滤重复数据），用户id，用户名，状态，问题id
        submissionId, userId, status, problemId = [], [], [], []
        for item in data["submissions"]:
            submissionId.append(item["id"])
            userId.append(item["userId"])
            status.append(item["status"])
            problemId.append(item["problemSetProblemId"])

        cur_user_id, cur_user_name, cur_school_id = [], [], []
        for student_key, student_info in data.get("studentUserById", {}).items():
            student_number = student_info.get("studentNumber", "")
            name = student_info.get("name", "")
            student_id = student_info.get("id", "")
            cur_user_id.append(student_id)
            cur_user_name.append(name)
            cur_school_id.append(student_number)

        return submissionId, userId, status, problemId, cur_user_id, cur_user_name, cur_school_id

    except requests.exceptions.Timeout:
        print("错误：请求超时，请检查网络或重试")
    except requests.exceptions.ConnectionError:
        print("错误：网络连接失败，请检查网络")
    except requests.exceptions.HTTPError as e:
        print(f"错误：HTTP请求失败，状态码 {e.response.status_code}")
        # 打印响应内容，便于排查问题（如未登录、权限不足）
        print(f"响应内容：{e.response.text}")
    except json.JSONDecodeError:
        print("错误：响应不是有效的JSON格式")
    except Exception as e:
        print(f"未知错误：{str(e)}")

    return None


def get_problem_types():
    """
    发起GET请求获取拼题啦题库的题目类型数据
    :return: dict/None  返回解析后的JSON数据（失败返回None）
    """
    try:
        # 发起GET请求（同步替换为新的变量名）
        response = requests.get(
            url=get_problem_url,
            params=get_problem_params,
            headers=get_problem_headers,
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
        print("❌ 错误：请求超时，请检查网络或重试")
    except requests.exceptions.ConnectionError:
        print("❌ 错误：网络连接失败，请检查网络")
    except requests.exceptions.HTTPError as e:
        print(f"❌ 错误：HTTP请求失败，状态码 {e.response.status_code}")
        print(f"❌ 响应内容：{e.response.text}")
    except json.JSONDecodeError:
        print("❌ 错误：响应不是有效的JSON格式")
    except Exception as e:
        print(f"❌ 未知错误：{str(e)}")

    return None