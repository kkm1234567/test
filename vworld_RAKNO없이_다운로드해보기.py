import requests
import json
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

def PostJson(session: requests.Session, url: str, payload: str, referer : str = None) -> dict | str:
        """
        URL로 JSON POST 요청을 보내고, JSON 응답을 딕셔너리로 반환합니다.

        :param url: 요청할 URL
        :param payload: 전송할 JSON 데이터 (dict)
        :param headers: 요청 헤더 (선택)
        :return: 서버 응답을 dict로 반환
        """
        # 기본 헤더: JSON 전송
        default_headers = {
            "Host": "www.vworld.kr",
            "Connection": "keep-alive",
            "Content-Length": str(len(payload.encode('utf-8'))),
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua-mobile": "?0",
            "Origin": "https://www.vworld.kr",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": referer,
            "Accept-Encoding": "gzip, deflate, br, zstd"
        }

        response = session.post(url, data=payload, headers=default_headers)

        response.raise_for_status()  # 실패 시 예외 발생

        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text  # BeautifulSoup 객체 대신 string 반환

def PostLoginJson() -> requests.Session:
    # 로그인
    session = requests.Session()

    # 1️⃣ 요청 파라미터
    payload = "usrIdeE=ZGF0YXNwYWNl&usrPwdE=c2owNyNxaHI%3D&nextUrl=%2Fv4po_main.do"

    # 4️⃣ POST 요청
    url = "https://www.vworld.kr/v4po_usrlogin_a004.do"
    responseString = PostJson(session, url, payload, "https://www.vworld.kr/v4po_usrlogin_a001.do")

    # if responseString["resultMap"]["msg"] != "로그인 성공":
    #     raise Exception("로그인에 실패하였습니다.")

    print("로그인에 성공하였습니다.")
    return session

def GetData(session, aaPageIndex):
    # 1️⃣ 요청 파라미터
    # 오늘 날짜
    today = datetime.today()

    # endDay = 오늘 날짜
    # end_day = today.strftime("%Y-%m-%d")
    end_day = "2018-12-31"

    # startDay = 오늘로부터 -12개월
    # start_day = (today - relativedelta(months=12)).strftime("%Y-%m-%d") 
    start_day = "2018-12-01"
    
    url = f"https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?usrIde=dataspace&pageSize=10&pageUnit=10&listPageIndex=1&gidsCd=&searchKeyword=%EC%97%B0%EC%86%8D%EC%A7%80%EC%A0%81%EB%8F%84&svcCde=NA&gidmCd=&searchBrmCode=&datIde=&searchFrm=&dsId=23&searchSvcCde=&searchOrganization=&dataSetSeq=23&searchTagList=&pageIndex=1&sortType=00&datPageIndex={aaPageIndex}&datPageSize=17&startDate={start_day}&endDate={end_day}&sidoCd=&fileGbnCd=AL&dsNm=&formatSelect="
    responseString = PostJson(session, url, "", "")
    return parse_html_items(responseString)   

def parse_html_items(responseString: str) -> list[dict]:

        soup = BeautifulSoup(responseString, "html.parser")
        html_items = []

        # list bd box hover 영역 찾기
        root = soup.select_one("div.list.bd.box.hover")
        if not root:
            print("[WARN] 대상 div.list.bd.box.hover 를 찾을 수 없습니다.")
            return []

        # 해당 영역 안 li만 선택
        list_items = root.select("ul > li")
        print(f"🔍 파싱할 전체 LI 개수: {len(list_items)}")


        for li in list_items:

            # ------------------------------------------------------
            # 1) title 파싱
            # ------------------------------------------------------
            title_el = li.select_one(".tit")
            if not title_el:
                continue

            title = title_el.text.strip()

            # 확장자 제거
            prefix = re.sub(r"\.zip$", "", title, flags=re.IGNORECASE)

            # ------------------------------------------------------
            # 2) dataMap 기반 필터링
            # ------------------------------------------------------

            #print(f"✅ 통과: {title} (prefix={prefix})")

            # ------------------------------------------------------
            # 3) 다운로드 파라미터 추출
            # ------------------------------------------------------
            btn = li.select_one(".btns button")
            dsFileId = None
            dsFileSq = None

            if btn:
                onclick = btn.get("onclick", "")
                params = re.findall(r"'([^']+)'", onclick)
                if len(params) >= 2:
                    dsFileId = params[0]
                    dsFileSq = params[1]

            # ------------------------------------------------------
            # 4) txt 정보 파싱
            # ------------------------------------------------------
            size = category = std_date = upd_date = None

            spans = li.select(".txt span")
            for sp in spans:
                text = sp.text.strip()

                if "용량" in text:
                    size = sp.select_one("em").text.strip()
                elif "구분" in text:
                    category = sp.select_one("em").text.strip()
                elif "기준일" in text:
                    std_date = sp.select_one("em").text.strip()
                elif "갱신일" in text:
                    upd_date = sp.select_one("em").text.strip()

            # ------------------------------------------------------
            # 5) append item
            # ------------------------------------------------------
            html_items.append({
                "title": title,
                "prefix": prefix,
                "dsFileId": dsFileId,
                "dsFileSq": dsFileSq,
                "size": size,
                "category": category,
                "std_date": std_date,
                "upd_date": upd_date,
            })

        print(f"🎯 최종 필터 후 결과 개수: {len(html_items)}")

        return html_items

def get_url_response(url, session=None):
    import requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        return response
    except Exception as e:
        print(f"[ERROR] GET 요청 실패: {e}")

def save_response_to_file(response, save_path):
    import os
    # 디렉토리 없으면 생성
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    # 바이너리로 저장
    with open(save_path, "wb") as f:
        f.write(response.content)
    print(f"파일 저장 완료: {save_path}")

def TruncateListAfterIndex(aoList: list, aiIndex: int) -> list:
    """
    리스트에서 지정된 인덱스 이후의 요소를 제거하여 반환.
    예: TruncateListAfterIndex([1,2,3,4,5], 2) → [1,2,3] (0~2 유지)
    음수 인덱스 지원: -1은 마지막 요소까지 유지.
    """
    if aiIndex < 0:
        aiIndex = len(aoList) + aiIndex
    return aoList[:aiIndex + 1]            

session = PostLoginJson()

PostLoginJson()

data = GetData(session, 1)
data = TruncateListAfterIndex(data, 16)

dsFileSqs = []
for item in data:
    dsFileId = str(item.get("dsFileId", ""))
    dsFileSq = str(item.get("dsFileSq", ""))
    dsFileSqs.append(dsFileId + dsFileSq)

base_url = "https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq="
joined_sqs = ",".join(dsFileSqs)
url = base_url + joined_sqs

# 1단계: VWorld URL GET
res = get_url_response(url, session)
save_response_to_file(res, r"C:\PTR\Prime\Collect\CollectApi\storage\vworld_kr\t_land_map\test.zip")