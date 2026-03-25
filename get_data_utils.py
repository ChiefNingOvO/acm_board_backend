import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

PTA_SUBMIT_URL = os.environ["PTA_SUBMIT_URL"]
PTA_PROBLEM_URL = os.environ["PTA_PROBLEM_URL"]
PTA_RANK_URL = os.environ["PTA_RANK_URL"]

PTA_COOKIE = os.environ["PTA_COOKIE"]
PTA_X_LOLLIPOP = os.environ["PTA_X_LOLLIPOP"]
PTA_X_MARSHMALLOW = os.environ["PTA_X_MARSHMALLOW"]
PTA_ACCEPT = os.environ["PTA_ACCEPT"]
PTA_ACCEPT_ENCODING = os.environ["PTA_ACCEPT_ENCODING"]
PTA_ACCEPT_LANGUAGE = os.environ["PTA_ACCEPT_LANGUAGE"]
PTA_CONTENT_TYPE = os.environ["PTA_CONTENT_TYPE"]
PTA_PRIORITY = os.environ["PTA_PRIORITY"]
PTA_REFERER_BASE_URL = os.environ["PTA_REFERER_BASE_URL"]
PTA_SEC_CH_UA = os.environ["PTA_SEC_CH_UA"]
PTA_SEC_CH_UA_MOBILE = os.environ["PTA_SEC_CH_UA_MOBILE"]
PTA_SEC_CH_UA_PLATFORM = os.environ["PTA_SEC_CH_UA_PLATFORM"]
PTA_SEC_FETCH_DEST = os.environ["PTA_SEC_FETCH_DEST"]
PTA_SEC_FETCH_MODE = os.environ["PTA_SEC_FETCH_MODE"]
PTA_SEC_FETCH_SITE = os.environ["PTA_SEC_FETCH_SITE"]
PTA_USER_AGENT = os.environ["PTA_USER_AGENT"]
PTA_SUBMISSIONS_REFERER_PATH = os.environ["PTA_SUBMISSIONS_REFERER_PATH"]
PTA_RANKINGS_REFERER_PATH = os.environ["PTA_RANKINGS_REFERER_PATH"]

PTA_SUBMIT_LIMIT = int(os.environ["PTA_SUBMIT_LIMIT"])
PTA_SUBMIT_FILTER = os.environ["PTA_SUBMIT_FILTER"]
PTA_PROBLEM_PARAMS_JSON = os.environ["PTA_PROBLEM_PARAMS_JSON"]
PTA_RANK_PAGE = int(os.environ["PTA_RANK_PAGE"])
PTA_RANK_LIMIT = int(os.environ["PTA_RANK_LIMIT"])
PTA_SUBMIT_TIMEOUT_SECONDS = float(os.environ["PTA_SUBMIT_TIMEOUT_SECONDS"])
PTA_PROBLEM_TIMEOUT_SECONDS = float(os.environ["PTA_PROBLEM_TIMEOUT_SECONDS"])
PTA_RANK_TIMEOUT_SECONDS = float(os.environ["PTA_RANK_TIMEOUT_SECONDS"])
PTA_ALLOW_REDIRECTS = os.environ["PTA_ALLOW_REDIRECTS"].lower() == "true"


def get_base_headers(referer_path: str) -> dict:
    return {
        "accept": PTA_ACCEPT,
        "accept-encoding": PTA_ACCEPT_ENCODING,
        "accept-language": PTA_ACCEPT_LANGUAGE,
        "content-type": PTA_CONTENT_TYPE,
        "cookie": PTA_COOKIE,
        "priority": PTA_PRIORITY,
        "referer": f"{PTA_REFERER_BASE_URL}/{referer_path}",
        "sec-ch-ua": PTA_SEC_CH_UA,
        "sec-ch-ua-mobile": PTA_SEC_CH_UA_MOBILE,
        "sec-ch-ua-platform": PTA_SEC_CH_UA_PLATFORM,
        "sec-fetch-dest": PTA_SEC_FETCH_DEST,
        "sec-fetch-mode": PTA_SEC_FETCH_MODE,
        "sec-fetch-site": PTA_SEC_FETCH_SITE,
        "user-agent": PTA_USER_AGENT,
        "x-lollipop": PTA_X_LOLLIPOP,
        "x-marshmallow": PTA_X_MARSHMALLOW,
    }


def get_submit_headers() -> dict:
    return get_base_headers(PTA_SUBMISSIONS_REFERER_PATH)


def get_problem_headers() -> dict:
    return get_base_headers(PTA_SUBMISSIONS_REFERER_PATH)


def get_rank_headers() -> dict:
    return get_base_headers(PTA_RANKINGS_REFERER_PATH)


get_submit_params = {
    "limit": PTA_SUBMIT_LIMIT,
    "filter": PTA_SUBMIT_FILTER,
}

get_problem_params = json.loads(PTA_PROBLEM_PARAMS_JSON)

get_rank_params = {
    "page": PTA_RANK_PAGE,
    "limit": PTA_RANK_LIMIT,
}


def get_pintia_submissions():
    print("[get_pintia_submissions] Starting PTA submission fetch...")
    try:
        response = requests.get(
            url=PTA_SUBMIT_URL,
            params=get_submit_params,
            headers=get_submit_headers(),
            timeout=PTA_SUBMIT_TIMEOUT_SECONDS,
            allow_redirects=PTA_ALLOW_REDIRECTS,
        )
        response.raise_for_status()

        data = response.json()
        submission_id_list, user_id_list, status_list, problem_id_list, judge_time_list = [], [], [], [], []
        for item in reversed(data["submissions"]):
            submission_id_list.append(item["id"])
            user_id_list.append(item["userId"])
            status_list.append(item["status"])
            problem_id_list.append(item["problemSetProblemId"])
            judge_time_list.append(item["submitAt"])

        current_user_id_list, current_user_name_list, current_school_id_list = [], [], []
        student_users = data.get("studentUserById") or {}
        for _, student_info in reversed(student_users.items()):
            if not student_info:
                continue
            current_user_id_list.append(student_info.get("id", ""))
            current_user_name_list.append(student_info.get("name", ""))
            current_school_id_list.append(student_info.get("studentNumber", ""))

        student_nick_id_list, student_real_id_list = [], []
        exam_members = data.get("examMemberByUserId") or {}
        for nick_id, nick_info in reversed(exam_members.items()):
            if not nick_info:
                continue
            student_nick_id_list.append(nick_id)
            student_real_id_list.append(nick_info.get("studentUserId", ""))

        return (
            submission_id_list,
            user_id_list,
            status_list,
            problem_id_list,
            judge_time_list,
            current_user_id_list,
            current_user_name_list,
            current_school_id_list,
            student_nick_id_list,
            student_real_id_list,
        )

    except requests.exceptions.Timeout:
        print("[get_pintia_submissions] Timeout while requesting PTA submissions.")
    except requests.exceptions.ConnectionError:
        print("[get_pintia_submissions] Connection error while requesting PTA submissions.")
    except requests.exceptions.HTTPError as e:
        print(f"[get_pintia_submissions] HTTP error: {e.response.status_code}")
        print(f"[get_pintia_submissions] Response body: {e.response.text}")
    except json.JSONDecodeError:
        print("[get_pintia_submissions] Response body is not valid JSON.")
    except Exception as e:
        print(f"[get_pintia_submissions] Unexpected error: {e}")

    return None


def get_problem_types():
    print("[get_problem_types] Starting PTA problem type fetch...")
    try:
        response = requests.get(
            url=PTA_PROBLEM_URL,
            params=get_problem_params,
            headers=get_problem_headers(),
            timeout=PTA_PROBLEM_TIMEOUT_SECONDS,
            allow_redirects=PTA_ALLOW_REDIRECTS,
        )
        response.raise_for_status()

        data = response.json()
        problem_id_list, label_list = [], []
        for item in data["labels"]:
            problem_id_list.append(item["id"])
            label_list.append(item["label"])
        return problem_id_list, label_list

    except requests.exceptions.Timeout:
        print("[get_problem_types] Timeout while requesting PTA problem types.")
    except requests.exceptions.ConnectionError:
        print("[get_problem_types] Connection error while requesting PTA problem types.")
    except requests.exceptions.HTTPError as e:
        print(f"[get_problem_types] HTTP error: {e.response.status_code}")
        print(f"[get_problem_types] Response body: {e.response.text}")
    except json.JSONDecodeError:
        print("[get_problem_types] Response body is not valid JSON.")
    except Exception as e:
        print(f"[get_problem_types] Unexpected error: {e}")

    return None


def get_common_rankings():
    try:
        response = requests.get(
            url=PTA_RANK_URL,
            params=get_rank_params,
            headers=get_rank_headers(),
            timeout=PTA_RANK_TIMEOUT_SECONDS,
            allow_redirects=PTA_ALLOW_REDIRECTS,
        )
        response.raise_for_status()

        data = response.json()
        id_to_name = {}
        for item in data["studentUserById"]:
            student = data["studentUserById"][item]
            id_to_name.setdefault(
                student["id"],
                f'{student["name"]} {student["studentNumber"]}',
            )

        rank_to_name = {}
        for item in data["commonRankings"]:
            rank_to_name.setdefault(
                item["rank"],
                id_to_name.get(item["user"]["studentUserId"]),
            )

        return rank_to_name

    except Exception as e:
        print(f"[get_common_rankings] Request failed: {e}")
        return None
