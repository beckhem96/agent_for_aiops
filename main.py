import os
import re
import json
import asyncio
import httpx
import redis.asyncio as redis

import requests
import uvicorn
import threading
import time
import base64

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from fastapi import FastAPI, Request, BackgroundTasks
from requests.auth import HTTPBasicAuth
from slack_sdk import WebClient
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from collections import defaultdict
from aiokafka.admin import AIOKafkaAdminClient
# ==========================================
# Slack 토큰 세팅
# ==========================================
load_dotenv()
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

ES_HOST = "http://localhost:9200"
REDIS_HOST = "redis://localhost:6379"
KAFKA_HOST = "localhost:9092"

# ==========================================
# 무한 루프 비동기 함수
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 백그라운드 스케줄러가 아닌, 비동기(AsyncIO) 스케줄러 사용
    scheduler = AsyncIOScheduler()

    # 각각의 타겟을 독립적인 타이머로 등록 (서로 딜레이 안 줌)
    scheduler.add_job(check_es_async, 'interval', seconds=30)  # ES는 30초마다
    scheduler.add_job(check_redis_async, 'interval', seconds=30)  # Redis도 30초마다
    scheduler.add_job(check_kafka_async, 'interval', seconds=30)  # Kafka도 30초마다

    scheduler.start()
    print("다중 타겟 비동기 헬스체크 스케줄러 가동 시작!")
    yield  # 여기서 FastAPI 서버가 메인으로 돌아갑니다.

    scheduler.shutdown()
    print("스케줄러 안전하게 종료됨")

# FastAPI 앱 & Slack 앱 초기화
fastapi_app = FastAPI(lifespan=lifespan)
slack_app = App(token=SLACK_BOT_TOKEN)
slack_client = WebClient(token=SLACK_BOT_TOKEN)
jira_auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

# ==========================================
# Elasticsearch 비동기 헬스체크
# ==========================================
async def check_es_async():
    print("[ES] 상태 확인 중...")
    try:
        # requests 대신 비동기 전용 httpx 사용! (FastAPI 멈춤 방지)
        async with httpx.AsyncClient() as client:
            health_res = await client.get(f"{ES_HOST}/_cluster/health", timeout=5.0)
            health_data = health_res.json()
            status = health_data.get("status")
            unassigned_count = health_data.get("unassigned_shards", 0)

            if status in ["yellow", "red"] and unassigned_count > 0:
                shards_res = await client.get(
                    f"{ES_HOST}/_cat/shards?format=json&h=index,shard,prirep,state,unassigned.reason",
                    timeout=5.0
                )
                all_shards = shards_res.json()

                # 'UNASSIGNED' 상태인 녀석들만 필터링
                broken_shards = [s for s in all_shards if s.get("state") == "UNASSIGNED"]

                # 에러 원인(Reason)별로 그룹핑하기 위한 딕셔너리 세팅
                # 예: {"NODE_LEFT": set("user-db", "order-db"), "ALLOCATION_FAILED": set("log-db")}
                reason_groups = defaultdict(set)
                rep_shards = {}  # 각 Reason별 대표 샤드 정보 저장 (진단서 떼기 용도)

                for s in broken_shards:
                    reason = s.get("unassigned.reason", "UNKNOWN_REASON")
                    idx_name = s.get("index")

                    reason_groups[reason].add(idx_name)

                    # 이 Reason의 대표 샤드를 아직 안 뽑았다면 하나 저장해둠
                    if reason not in rep_shards:
                        rep_shards[reason] = {
                            "index": idx_name,
                            "shard": int(s.get("shard", 0)),
                            "primary": True if s.get("prirep") == "p" else False
                        }
                # 그룹별로 ES에게 심층 진단(explain) 요청하기
                group_reports = []
                for reason, indices in reason_groups.items():
                    rep = rep_shards[reason]

                    # 특정 인덱스의 특정 샤드를 콕 집어서 "얘 왜이래?" 하고 물어보는 POST 요청
                    explain_body = {
                        "index": rep["index"],
                        "shard": rep["shard"],
                        "primary": rep["primary"]
                    }
                    explain_res = await client.post(
                        f"{ES_HOST}/_cluster/allocation/explain",
                        json=explain_body,
                        timeout=5.0
                    )

                    explanation = "진단 설명 없음"
                    if explain_res.status_code == 200:
                        explanation = explain_res.json().get("allocate_explanation", "진단 설명 없음")

                    # 이 그룹의 리포트 블록 만들기
                    indices_str = ", ".join(list(indices))
                    group_report = (
                        f"*에러 분류(Reason):* `{reason}`\n"
                        f"   - *영향받은 인덱스:* `{indices_str}`\n"
                        f"   - *ES 진단:* `{explanation}`"
                    )
                    group_reports.append(group_report)

                # 슬랙용 최종 메시지 조립
                reports_str = "\n\n".join(group_reports)
                error_msg = (
                    f"*[Elasticsearch 장애 리포트]*\n"
                    f"- *클러스터 상태:* `{status.upper()}`\n"
                    f"- *총 미할당 샤드:* `{unassigned_count}` 개\n\n"
                    f"*[장애 원인별 그룹 분석]*\n"
                    f"{reports_str}"
                )
                print(error_msg)
                slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)

            elif status in ["yellow", "red"]:
                print(f"🚨 [ES 경고] 상태: {status} (샤드 외 문제)")
            else:
                print(f"✅ [ES 정상] status: green")
    except Exception as e:
        print(f"[ES 다운 의심] 연결 실패: {e}")
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"[ES 다운 의심] 연결 실패: {e}")



# ==========================================
# Redis 비동기 헬스체크
# ==========================================
async def check_redis_async():
    print("[Redis] 상태 확인 중...")
    try:
        # 비동기 Redis 클라이언트 연결
        r = redis.from_url(REDIS_HOST)
        # 1차 진단: INFO(현재 상태)와 CONFIG(설정값 한계치) 둘 다 가져오기
        info = await r.info('all')
        config = await r.config_get('*')

        used_memory = info.get("used_memory_human")
        max_memory = int(config.get("maxmemory", 0))
        mem_fragmentation = info.get("mem_fragmentation_ratio", 0)
        rejected_conn = info.get("rejected_connections", 0)
        connected_clients = info.get("connected_clients", 0)
        max_clients = int(config.get("maxclients", 10000))
        evicted_keys = info.get("evicted_keys", 0) # 꽉 차서 강제로 지워진 데이터 수

        anomaly_detected = False
        diagnostics = []

        # 심층 진단 로직 분기 (3대장 잡기)
        # Case A: 메모리 폭발(OOM 직접 or 강제 삭제중)
        if (max_memory > 0 and used_memory / max_memory > 0.9) or evicted_keys >0:
            anomaly_detected = True
            diagnostics.append(
                f"*메모리 포화 (OOM 경고)*\n"
                f"   - *상태:* 사용량 `{info.get('used_memory_human')}` / 한계치 `{max_memory} bytes`\n"
                f"   - *강제 삭제(Evicted):* `{evicted_keys}` 건\n"
                f"   - *진단:* 설정된 maxmemory에 도달하여 기존 캐시 데이터 유실이 발생 중입니다. 메모리 증설 또는 TTL(만료 시간) 점검이 시급합니다."
            )
        # Case B: 커넥션 풀 고갈 (앱에서 Redis로 접근 불가)
        if connected_clients >= max_clients * 0.9 or rejected_conn > 0:
            anomaly_detected = True
            diagnostics.append(
                f"*커넥션 고갈 (연결 거부)*\n"
                f"   - *상태:* 현재 연결 `{connected_clients}` / 최대 허용 `{max_clients}`\n"
                f"   - *거절된 연결 누적:* `{rejected_conn}` 건\n"
                f"   - *진단:* Java/Spring 앱의 Connection Pool 설정이 너무 작거나, 커넥션 누수(Close 안 함)가 의심됩니다."
            )
        # Case C: 슬로우 쿼리 (어디서 싱글 스레드를 막고 있나?)
        # 에러가 감지 되었다면, 범인을 잡기위해 최근 가장 느렸던 명령어 3개를 털어옴
        if anomaly_detected:
            slowlogs = await r.slowlog_get(3)
            if slowlogs:
                slow_cmds = []
                for log in slowlogs:
                    # byte를 string으로 변환해서 명령어 조합
                    cmd = " ".join([m.decode('utf-8') if isinstance(m, bytes) else str(m) for m in log['command']])
                    duration_ms = log['duration'] / 1000.0
                    slow_cmds.append(f"`{cmd}` ({duration_ms}ms)")

                diagnostics.append(
                    f"*최근 병목 유발 명령어 (SlowLog)*\n   - " +
                    "\n   - ".join(slow_cmds) +
                    f"\n   - *진단:* Redis는 싱글 스레드입니다. 위 무거운 명령어(`KEYS *` 등)가 전체 시스템을 멈추게 했을 확률이 매우 높습니다."
                )
        # Case D: 메모리 파편화(효율성 최악 상태)
        # 비율이 1.5 이상이면 물리적 메모리가 심각하게 낭비되고 있다는 뜻
        if mem_fragmentation > 1.5:
            anomaly_detected = True
            diagnostics.append(
                f"*메모리 파편화 심각 (조각모음 필요)*\n"
                f"   - *파편화율(Ratio):* `{mem_fragmentation:.2f}` (정상 범위: 1.0 ~ 1.5)\n"
                f"   - *진단:* 실제 데이터 크기보다 물리적 메모리를 너무 많이 점유하고 있습니다. Redis의 `activedefrag yes` 설정 활성화 또는 서비스 안정 시점에 재시작을 권장합니다.\n"
            )
        # 최종 메시지 조립
        if anomaly_detected:
            reports_str = "\n\n".join(diagnostics)
            error_msg = (
                f"*[Redis 장애 리포트]*\n"
                f"- *타겟 서버:* `{REDIS_HOST}`\n\n"
                f"*[장애 원인 분석]*\n"
                f"{reports_str}"
            )
            print(error_msg)
            slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
        else:
            print(f"[Redis 정상] 메모리 및 커넥션 안정적")
        await r.aclose()  # 연결 예쁘게 닫기
    except Exception as e:
        print(f"[Redis 다운 의심] 연결 실패: {e}")
        # slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"[Redis 다운 의심] 연결 실패: {e}")

# ==========================================
# Kafka 비동기 헬스체크 (초경량 TCP Socket 체크)
# ==========================================
async def check_kafka_async():
    print("[Kafka] 상태 확인 중...")
    # 카프카 관리자 (admin) 클라인트 생성
    admin_client = AIOKafkaAdminClient(bootstrap_servers=KAFKA_HOST)

    try:
        # 연결 시도 (여기서 뻗으면 브로커 자체가 죽은 것!)
        await admin_client.start()

        # 클러스터 메타데이터 털어오기 (토픽과 파티션 정보)
        topics = await admin_client.list_topics()

        if not topics:
            print("[Kafka 정상] 생성된 토픽이 없습니다 (브로커 정상)")
            await admin_client.close()
            return

        # 모든 토픽의 상세 상태를 가져온다.
        topic_details = await admin_client.describe_topics(topics)

        offline_partitions = []
        under_replicated_partitions = []

        # 심층 진단 로직: 아픈 파티션 색출하기
        for topic in topic_details:
            topic_name = topic['topic']
            for partition in topic['partitions']:
                part_id = partition['partition']
                leader = partition['leader']
                replicas = partition['replicas']
                isr = partition['isr'] # In-Sync Replicas (정상적으로 복제 중인 노드emf)

                # Case A: 리더가 없음 (Offline Partition) -> 당장 데이터 읽기/쓰기 불가능! (대형 장애)
                if leader == -1 or leader is None:
                    offline_partitions.append(f"`{topic_name}` (파티션: {part_id}")
                # Case3 B: 복제분이 깨짐 (Under-replicated) -> 당장 멈추진 않지만 노드 하나 더 죽으면 데이터 날아감
                elif len(isr) < len(replicas):
                    under_replicated_partitions.append(f"`{topic_name}` (파티션: {part_id} | 정상복제: {len(isr)}/{len(replicas)})")
        diagnostics = []

        if offline_partitions:
            diagnostics.append(
                f"*오프라인 파티션 발생 (데이터 입출력 불가!)*\n"
                f"   - *영향받은 토픽:* {', '.join(offline_partitions)}\n"
                f"   - *진단:* 리더 브로커가 다운되어 해당 파티션의 읽기/쓰기가 멈췄습니다. 즉시 브로커를 재시작하거나 컨트롤러 상태를 확인하세요."
            )
        if under_replicated_partitions:
            diagnostics.append(
                f"*파티션 복제 지연/실패 (고가용성 위험)*\n"
                f"   - *영향받은 토픽:* {', '.join(under_replicated_partitions)}\n"
                f"   - *진단:* 설정된 복제본(Replica) 수만큼 데이터가 동기화되지 않고 있습니다. 특정 노드의 디스크나 네트워크 장애가 의심됩니다."
            )
        if diagnostics:
            reports_str = "\n\n".join(diagnostics)
            error_msg = (
                f"*[Kafka 장애 리포트]*\n"
                f"- *타겟 클러스터:* `{KAFKA_HOST}`\n\n"
                f"- *[장애 원인 분석]*\n"
                f"{reports_str}"
            )
            print(error_msg)
            slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"{error_msg}")
        else:
            print(f"[Kafka 정상] 모든 토픽/파티션 복제 상태 안정적")
        await admin_client.close()
    except Exception as e:
        error_msg = (
            f"*[Kafka 브로커 다운 의심]* 💥\n"
            f"- *접속 불가:* `{KAFKA_HOST}`\n"
            f"- *에러 내용:* `{e}`\n"
            f"> _Kafka 프로세스가 죽었거나 네트워크 방화벽이 막혔습니다!_"
        )
        print(error_msg)
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"[Kafka 다운 의심] 브로커 응답 없음: {e}")
        # slack_client.chat_postMessage(...)

# ==========================================
# ES 상태 확인하는 함수
# ==========================================
# async def check_elasticsearch_periodic():
#     while True:
#         print("⏳ [1분 주기] Elasticsearch 헬스체크 중...")
#         try:
#             # ES의 클러스터 헬스 API를 찌릅니다. (5초 안에 답 없으면 에러 처리)
#             # requests.get은 '동기(Blocking)' 함수라 웨이터를 멈추게 합니다.
#             # 그래서 asyncio.to_thread()로 "주방보조(별도 스레드)한테 맡기고 웨이터는 바로 복귀해!" 라고 명령합니다.
#             response = await asyncio.to_thread(requests.get, f"{ES_HOST}/_cluster/health", timeout=5)
#
#             if response.status_code == 200:
#                 data = response.json()
#                 status = data.get("status")
#                 unassigned = data.get("unassigned_shards", 0)
#
#                 # 🚨 에러 조건: 상태가 green이 아니거나, 길 잃은 샤드가 있을 때!
#                 if status in ["yellow", "red"] or unassigned > 0:
#                     error_msg = f"🚨 *[ES 장애 감지]*\n- *상태:* {status}\n- *Unassigned Shards:* {unassigned}\n- *확인 요망!*"
#                     print(error_msg)
#
#                     # 여기서 기존에 만들어두신 슬랙 봇으로 전송!
#                     slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
#                 else:
#                     print(f"✅ [ES 정상] 현재 상태: {status}")
#             else:
#                 print(f"❌ ES 응답 에러: HTTP {response.status_code}")
#
#         except Exception as e:
#             # 아예 ES 서버가 죽어서 통신조차 안 될 때! (이게 진짜 크리티컬)
#             fatal_msg = f"💥 *[ES 서버 다운 의심]*\n- 접속 불가: `{ES_HOST}`\n- 에러 내용: {e}"
#             print(fatal_msg)
#             slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=fatal_msg)
#         await asyncio.sleep(60)

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
def parse_message_to_format(issue_id: str, user_text: str, title_text: str, image_data=None, is_jira=False):
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
        scenario_match = re.search(r'시나리오[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|증상|빈도|기대결과)|$)', user_text, flags=re.DOTALL)
        error = re.search(r'증상[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|기대결과|시나리오|빈도)|$)', user_text, flags=re.DOTALL)
        expected_match = re.search(r'기대결과[\s:\-\]]*(.*?)(?=\n\s*\[?(?:제목|에러|기대결과|시나리오|빈도|증상)|$)', user_text,
                                   flags=re.DOTALL)
        print(issue_id)
        formatted_data = {
            "id" : issue_id,
            "host": JIRA_SERVER,
            "title": clean_text(title_text).strip() if title_text else "제목 없음 (파싱 불가)",
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
            "id" : issue_id,
            "host": hostname,
            "title": "",
            "type": msg_type,
            "env": env,
            "scenario": "",
            "error": clean_text(extracted_error),
            "expected_result": ""
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
        formatted_data = parse_message_to_format(issue_id=issue_key, user_text=description, title_text=summary, image_data=base64_images_list, is_jira=True)

        # 3️⃣ 예쁘게 포맷팅된 데이터를 외부 API로 쏩니다! 🚀
        target_api_url = "https://httpbin.org/post"
        api_response = requests.post(
            target_api_url,
            json=formatted_data,
            headers={"Content-Type": "application/json"}
        )

        # slack_client.chat_postMessage(
        #     channel=SLACK_CHANNEL_ID,
        #     text=f"```{formatted_data + "....."}```"
        # )
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
        formatted_data = parse_message_to_format("", user_text, "")

        # 2️⃣ 외부 API로 Request 전송 (httpbin 테스트)
        target_api_url = "https://httpbin.org/post"
        response = requests.post(target_api_url, json=formatted_data, headers={"Content-Type": "application/json"})

        # 3️⃣ API 응답 결과 슬랙에 즉시 보고
        if response.status_code == 200:
            api_result = response.json()

            dummy = json.dumps(api_result.get('json'), ensure_ascii=False, indent=2)
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
