import os
import re
import json
import requests
import uvicorn
import threading
import time
import base64

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from fastapi import FastAPI, Request, BackgroundTasks
from requests.auth import HTTPBasicAuth
from slack_sdk import WebClient
# ==========================================
# Slack 토큰 세팅
# ==========================================
# 앱 설정 페이지에서 발급받은 토큰들을 환경변수나 여기에 직접 넣으세요.
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

# ==========================================
# Jira 세팅
# ==========================================
JIRA_SERVER = os.getenv("JIRA_SERVER")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

# FastAPI 앱 & Slack 앱 초기화
fastapi_app = FastAPI()
slack_app = App(token=SLACK_BOT_TOKEN)
slack_client = WebClient(token=SLACK_BOT_TOKEN)
jira_auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

def clean_text(text: str) -> str:
    if not text:
        return "파싱 불가"
        # !.*?\.(?:png|jpg|gif|jpeg).*?! :
    # !로 시작하고, 중간에 점(.)과 이미지 확장자가 있고, !로 끝나는 패턴 매칭 (비탐욕적)
    jira_img_pattern = r'!.*?\.(?:png|jpg|gif|jpeg).*?!'
    cleaned_markup = re.sub(jira_img_pattern, '', text, flags=re.DOTALL)

    # \s+ 는 줄바꿈(\n), 탭(\t), 여러 개의 연속된 띄어쓰기를 전부 띄어쓰기 한 칸( )으로 바꿉니다.
    return re.sub(r'\s+', ' ', cleaned_markup).strip()

def clean_text2(text: str) -> str:
    if not text:
        return "파싱 불가"
        # !.*?\.(?:png|jpg|gif|jpeg).*?! :
    # !로 시작하고, 중간에 점(.)과 이미지 확장자가 있고, !로 끝나는 패턴 매칭 (비탐욕적)
    jira_img_pattern = r'!.*?\.(?:png|jpg|gif|jpeg).*?!'
    cleaned_markup = re.sub(jira_img_pattern, '', text, flags=re.DOTALL)

    return cleaned_markup

# ==========================================
# 2. ⚡ 규칙 기반 파싱 에이전트 (No LLM!)
# ==========================================
def parse_message_to_format(user_text: str, image_data=None, is_jira=False) -> dict:
    """
    LLM 없이 오직 키워드와 정규식 규칙(Rule)만으로 데이터를 추출합니다.
    (속도 0.001초 컷!)
    """
    text_lower = user_text.lower()
    print(user_text)
    # 1. 유형(Type) 분류: 단순 키워드 매칭
    if any(keyword in text_lower for keyword in ["apm", "장애", "cpu", "메모리", "알람"]):
        msg_type = "apm"
    elif is_jira or any(keyword in text_lower for keyword in ["jira", "티켓", "이슈", "kan-"]):
        msg_type = "jira"
    else:
        msg_type = "voc"  # APM도 Jira도 아니면 일반 고객/사용자 VOC로 간주

    # 2. 환경(Env) 분류: 키워드 매칭
    if any(keyword in text_lower for keyword in ["운영", "prod", "prd", "상용"]):
        env = "prod"
    else:
        env = "dev"  # 기본값은 안전하게 dev로 세팅

    # 3. 데이터 추출 (정규식 활용)
    # 슬랙에 "제목: ~~~", "시나리오: ~~~" 형태로 들어온다고 가정한 파싱 로직입니다.
    # 콜론(:) 앞뒤의 띄어쓰기까지 유연하게 잡아냅니다.
    formatted_data = {}
    if msg_type == "voc" or msg_type == "jira":
        title_match = re.search(r'제목[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|시나리오|기대결과|환경|증상)|$)', user_text, flags=re.DOTALL)
        scenario_match = re.search(r'시나리오[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|증상|빈도|기대결과)|$)', user_text, flags=re.DOTALL)
        error = re.search(r'증상[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|기대결과|시나리오|빈도)|$)', user_text, flags=re.DOTALL)
        expected_match = re.search(r'기대결과[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|에러|기대결과|시나리오|빈도|증상)|$)', user_text,
                                   flags=re.DOTALL)

        formatted_data = {
            "title": clean_text(title_match.group(1)).strip() if title_match else "제목 없음 (파싱 불가)",
            "type": msg_type,
            "env": env,
            "scenario": clean_text(scenario_match.group(1)).strip() if scenario_match else "파싱 불가",
            # 💡 [수정됨] 기존 코드에 error 변수 조건이 scenario_match로 되어있던 오타 수정!
            "error": clean_text(error.group(1)).strip() if error else "파싱 불가",
            "expected_result": clean_text(expected_match.group(1)).strip() if expected_match else "파싱 불가"
        }

        # 🌟 핵심: Jira 타입일 때만 이미지 데이터를 추가합니다!
        if msg_type == "jira":
            # API 요구사항에 맞춰 빈 문자열("")이나 null(None)로 처리 가능
            formatted_data["img"] = image_data if image_data else ""

    else:
        log_match = re.search(r'```(.*?)```', user_text, flags=re.DOTALL)
        if log_match:
            extracted_error = log_match.group(1).strip()
        else:
            extracted_error = user_text.replace("APM 서버 에러 알림", "").strip()

        formatted_data = {
            "type": msg_type,
            "env": env,
            "error": clean_text(extracted_error)
        }

    return formatted_data


def process_jira_webhook(payload: dict):
    try:
        # 1️⃣ 웹훅 페이로드에서는 오직 '이슈 키'만 훔쳐옵니다! (나머지 빈약한 데이터는 안 믿음)
        issue_key = payload.get("issue", {}).get("key")

        if not issue_key:
            print("❌ 웹훅 페이로드에 이슈 키가 없습니다.")
            return

        print(f"[{issue_key}] 알림 수신 완료! 첨부파일 업로드 대기를 위해 2초 대기합니다...")

        # 2️⃣ 🌟 핵심: Jira가 첨부파일을 완전히 저장할 수 있게 2초 정도 기다려줍니다.
        time.sleep(2)

        clean_server_url = JIRA_SERVER.rstrip('/')
        url = f"{clean_server_url}/rest/api/2/issue/{issue_key}"

        print(f"[{issue_key}] API 찌르는 중... URL: {url}")

        response = requests.get(url, auth=jira_auth, headers={"Accept": "application/json"})

        # 🚨 여기서 에러의 진짜 원인을 잡아냅니다!
        if response.status_code != 200:
            print(f"❌ Jira 상세 조회 실패 (HTTP {response.status_code})\n응답 내용: {response.text}")
            return

        try:
            full_issue_data = response.json()
        except Exception as e:
            # 상태 코드는 200 OK 인데 JSON이 아닐 경우 (보안/로그인 페이지 등)
            print(f"❌ JSON 파싱 에러! Jira가 데이터를 안 주고 이상한 걸 줬습니다:\n{response.text[:500]}")
            return

        fields = full_issue_data.get("fields", {})

        summary = fields.get("summary", "제목 없음")
        description = fields.get("description", "본문 없음")  # 이제 본문도 100% 나옵니다!
        attachments = fields.get("attachment", [])  # 첨부파일도 무조건 들어있습니다!

        print(f"[{issue_key}] 본문 글자수: {len(description)}, 첨부파일: {len(attachments)}개 확인!")

        # 4️⃣ 첨부파일이 없을 경우: 텍스트만 슬랙으로 쏨
        if not attachments:
            slack_client.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=f"새로운 이슈 등록: <{JIRA_SERVER}/browse/{issue_key}|{issue_key}>*\n*제목:* {summary}\n*본문:*\n```{description}```"
            )
            return

        file_uploads_list = []
        base64_images_list = []
        # 5️⃣ 첨부파일이 있을 경우: 다운로드 후 슬랙으로 파일 업로드
        for att in attachments:
            file_name = att.get("filename")
            download_url = att.get("content")

            img_response = requests.get(download_url, auth=jira_auth)

            if img_response.status_code == 200:
                print("✅ 이미지 다운로드 성공!")
                file_uploads_list.append({
                    "file": img_response.content,  # 다운받은 바이트 데이터
                    "filename": file_name  # 파일 이름
                })
                # 1️⃣ 이미지 바이트 데이터를 JSON용 텍스트(Base64)로 변환
                img_base64_str = base64.b64encode(img_response.content).decode('utf-8')
                base64_images_list.append(img_base64_str)
            else:
                print(f"❌ 첨부파일 다운로드 실패 (HTTP {img_response.status_code})")
        if file_uploads_list:
            print(f"🚀 총 {len(file_uploads_list)}개의 파일을 슬랙으로 한 번에 전송합니다...")
            slack_client.files_upload_v2(
                channel=SLACK_CHANNEL_ID,
                initial_comment=f"새로운 이슈 등록: <{JIRA_SERVER}/browse/{issue_key}|{issue_key}>\n*제목:* {summary}\n*본문:*\n{clean_text2(description)}",
                file_uploads=file_uploads_list  # 🌟 핵심: file= 대신 file_uploads= 에 리스트를 통째로 넘김!
            )

        # 2️⃣ 아까 만든 파싱 함수에 텍스트(description)와 이미지(Base64)를 같이 던져줍니다!
        formatted_data = parse_message_to_format(user_text=description, image_data=base64_images_list, is_jira=True)

        # 3️⃣ 예쁘게 포맷팅된 데이터를 외부 API로 쏩니다! 🚀
        target_api_url = "https://httpbin.org/post"
        api_response = requests.post(
            target_api_url,
            json=formatted_data,
            headers={"Content-Type": "application/json"}
        )
        slack_client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=f"```{api_response.json()['data'][:1000] + "....."}```"
        )
        print(f"API 전송 완료! HTTP 상태 코드: {api_response.status_code}")

    except Exception as e:
        print(f"🚨 처리 중 에러 발생: {e}")

# ==========================================
# 3. 메인 로직 (슬랙 메시지 수신부)
# ==========================================
# @app.event("app_mention")
@slack_app.event("message")
def handle_mention(event, say):
    user_text = event['text']

    try:
        # 1️⃣ 정규식 파서 가동!
        formatted_data = parse_message_to_format(user_text)

        # 2️⃣ 외부 API로 Request 전송 (httpbin 테스트)
        target_api_url = "https://httpbin.org/post"
        response = requests.post(target_api_url, json=formatted_data, headers={"Content-Type": "application/json"})

        # 3️⃣ API 응답 결과 슬랙에 즉시 보고
        if response.status_code == 200:
            api_result = response.json()

            dummy = json.dumps(api_result.get('json'), ensure_ascii=False, indent=2)
            # say(final_message)
            say(f"```{dummy}```")
            print(dummy)
        else:
            say(f"❌ **API 전송 실패!** (HTTP {response.status_code})")

    except Exception as e:
        say(f"🚨 처리 중 에러 발생:\n```{str(e)}```")

@fastapi_app.post("/jira-to-slack")
async def handle_jira_push(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    background_tasks.add_task(process_jira_webhook, payload)
    return {"status": "success"}

def start_slack_socket_mode():
    """슬랙 소켓 모드를 백그라운드에서 실행하는 함수"""
    print("⚡️ 슬랙 소켓 모드 일꾼 가동!")
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    handler.start()

@fastapi_app.on_event("startup")
def startup_event():
    """FastAPI 서버가 켜질 때, 슬랙 봇도 별도의 스레드로 같이 켭니다."""
    thread = threading.Thread(target=start_slack_socket_mode, daemon=True)
    thread.start()

if __name__ == "__main__":
    print("⚡️ 초고속 파싱 에이전트 봇 가동 완료! (No LLM)")
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)