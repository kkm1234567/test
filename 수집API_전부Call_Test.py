import json
import requests
from copy import deepcopy

domain = "http://localhost:8080"

# TaskGroup → TaskName 리스트 매핑
task_map = {

    # "mois_go_kr": [
    #     "mois_legal_dong",
    # ],

    # "juso_go_kr": [
    #     "juso_road_name",
    #     "juso_road_name_address",
    #     "juso_road_name_building",
    #     "juso_road_name_building_room",
    #     "juso_road_name_land_address",
    # ],


    "vworld_kr": [
        # "vworld_land_forest_address",
        # "vworld_doro_name_juso_move",
        "vworld_land_forest_land_move",
        # "vworld_land_forest_plan",
    ]
}

# 원본 payload


base_payload = {
    "Domain": "Unity/Collect",
    "Environ": "dev",
    "Creator": "PTR.Prime.Collect.CollectApi.kkm4512",
    "JobGroup": "",
    "JobName": "",
    "BeginTime": "",
    "StatusCode": "",
    "StatusText": "",
    "LogKey": "5cdcf426-3e22-4ccf-a818-b859da87d099",
    "Task": [
        {
            "TaskSeq": 1,
            "TaskGroup": "",
            "TaskName": "",
            "ProcessModel": "In/Out",
            "TaskType": "TaskCommand",
            "TaskDictionary": [],
            "TaskQuery": [],
            "ServerName": "krServer24",
            "Version": "1.0.9211.36117",
            "BeginTime": "2025-11-25T18:29:00.8:00",
            "RunTimeout": 100,
            "StatusCode": "",
            "StatusText": "",
        }
    ],
    "JobLogs": []
}

url = f"{domain}/jobs/execute"


def run_all_jobs():
    """TaskGroup별 TaskName 목록을 돌며 API 호출"""

    for task_group, task_names in task_map.items():

        print(f"\n===============================")
        print(f"🔹 TaskGroup 실행: {task_group}")
        print(f"===============================\n")

        for task_name in task_names:

            payload = deepcopy(base_payload)

            # Job 전체 정보
            payload["JobGroup"] = task_group
            payload["JobName"] = task_name

            # Task 내부 정보
            payload["Task"][0]["TaskGroup"] = task_group
            payload["Task"][0]["TaskName"] = task_name

            print(f"▶ 호출: {task_group} → {task_name}")

            response = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )

            print("HTTP Status:", response.status_code)
            print("Response:", response.text)
            print("--------------------------------\n")


if __name__ == "__main__":
    run_all_jobs()
