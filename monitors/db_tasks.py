import asyncio
import httpx
import redis.asyncio as redis
import asyncpg
import os

from collections import defaultdict
from aiokafka.admin import AIOKafkaAdminClient
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import ReadTimeout, WriteTimeout, Unavailable, OperationTimedOut
from slack_bolt import App
from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()
# 앱 설정 페이지에서 발급받은 토큰들을 환경변수나 여기에 직접 넣으세요.
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

ES_HOST = "http://localhost:9200"
REDIS_HOST = "redis://localhost:6379"
KAFKA_HOST = "localhost:9092"
CASSANDRA_HOST = "localhost"
PG_URL = "postgresql://postgres:mysecretpassword@localhost:5432/postgres"

slack_app = App(token=SLACK_BOT_TOKEN)
slack_client = WebClient(token=SLACK_BOT_TOKEN)
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
                        explain_data = explain_res.json()
                        explanation = explain_data.get("allocate_explanation", "진단 설명 없음")
                        # 추가된 디테일 파싱 로직
                        # ES가 "노드별 설명을 보라"고 핑계를 대면, 진짜 거절 사유를 뽑아옵니다.
                        if "node-by-node explanation" in explanation:
                            decisions = explain_data.get("node_allocation_decisions", [])
                            if decisions:
                                # 첫 번째 노드가 샤드 할당을 거절한(NO) 진짜 이유를 찾습니다.
                                deciders = decisions[0].get("deciders", [])
                                if deciders:
                                    detailed_reason = deciders[0].get("explanation", "상세 원인 불명")
                                    # 겉핥기 설명 대신, 진짜 이유를 메인으로 덮어씁니다
                                    explanation = f"{detailed_reason}"

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
                print(f"[ES 경고] 상태: {status} (샤드 외 문제)")
            else:
                print(f"[ES 정상] status: green")
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

        used_memory = int(info.get("used_memory", 0))
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
        # Case D: 메모리 파편화(효율성 최악 상태)
        # 비율이 1.5 이상이면 물리적 메모리가 심각하게 낭비되고 있다는 뜻
        if mem_fragmentation > 1.5 and used_memory > (10 * 1024 * 1024):
            # Redis 내부의 진단 확인
            try:
                # bytes를 string으로 변환
                doctor_report = await r.execute_command('MEMORY', 'DOCTOR')
                if isinstance(doctor_report, bytes):
                    doctor_report = doctor_report.decode('utf-8')
            except Exception:
                doctor_report = "Memory Doctor 기능을 지원하지 않거나 권한이 없습니다."

            anomaly_detected = True
            diagnostics.append(
                f"*메모리 파편화 심각 (조각모음 필요)*\n"
                f"   - *파편화율(Ratio):* `{mem_fragmentation:.2f}` (정상 범위: 1.0 ~ 1.5)\n"
                f"   - *Redis Doctor 공식 진단:*\n```text\n{doctor_report}\n```\n"
                f"   - *해결책:* Redis의 `activedefrag yes` 설정 활성화 또는 재시작 권장."
            )

        # Case C: 슬로우 쿼리 (어디서 싱글 스레드를 막고 있나?)
        # 에러가 감지 되었다면, 범인을 잡기위해 최근 가장 느렸던 명령어 3개를 털어옴
        if anomaly_detected:
            slowlogs = await r.slowlog_get(3)
            if slowlogs:
                slow_cmds = []
                for log in slowlogs:
                    raw_cmd = log.get('command')

                    # 아스키코드 대참사 방지용 완벽 디코딩 로직 🌟
                    if isinstance(raw_cmd, bytes):
                        # 1. 통짜 바이트 덩어리로 올 때
                        cmd = raw_cmd.decode('utf-8', errors='replace')
                    elif isinstance(raw_cmd, str):
                        # 2. 이미 문자열일 때
                        cmd = raw_cmd
                    elif isinstance(raw_cmd, list):
                        # 3. 단어별 리스트로 쪼개져서 올 때
                        cmd = " ".join(
                            [m.decode('utf-8', errors='replace') if isinstance(m, bytes) else str(m) for m in raw_cmd])
                    else:
                        cmd = str(raw_cmd)

                    # 슬랙 도배 방지: 명령어가 80자를 넘어가면 자르기 (...)
                    if len(cmd) > 80:
                        cmd = cmd[:80] + " ... (생략)"

                    duration_ms = log['duration'] / 1000.0
                    slow_cmds.append(f"`{cmd}` ({duration_ms}ms)")

                diagnostics.append(
                    f"*최근 병목 유발 명령어 (SlowLog)*\n   - " +
                    "\n   - ".join(slow_cmds) +
                    f"\n   - *진단:* Redis는 싱글 스레드입니다. 위 무거운 명령어(`KEYS *` 등)가 전체 시스템을 멈추게 했을 확률이 매우 높습니다."
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

        # 클러스터 기본 정보 가져오기
        cluster_info = await admin_client.describe_cluster()
        active_brokers = len(cluster_info.get('brokers', []))

        anomaly_detected = False
        diagnostics = []

        # 클러스터 메타데이터 털어오기 (토픽과 파티션 정보)
        topics = await admin_client.list_topics()
        if topics:
            # 변수를 미리 초기화해서 에러 방지
            topics_details = await admin_client.describe_topics(topics)

            # 심층 진단 로직: 아픈 파티션 색출하기
            for topic in topics_details:
                topic_name = topic['topic']
                for partition in topic['partitions']:
                    part_id = partition['partition']
                    leader = partition['leader']
                    replicas = partition['replicas']
                    isr = partition['isr'] # In-Sync Replicas (정상적으로 복제 중인 노드emf)

                    # Case A: 리더가 없음 (Offline Partition) -> 당장 데이터 읽기/쓰기 불가능! (대형 장애)
                    if leader == -1:
                        anomaly_detected = True
                        diagnostics.append(f"*[데이터 마비] 리더 상실* (Topic: {topic_name}, P: {part_id})")
                    # Case3 B: 복제분이 깨짐 (Under-replicated) -> 당장 멈추진 않지만 노드 하나 더 죽으면 데이터 날아감
                    elif len(isr) < len(replicas):
                        anomaly_detected = True
                        missing_count = len(replicas) - len(isr)
                        diagnostics.append(
                            f"*[복제 위험] Under-Replicated*\n"
                            f"   • *토픽:* `{topic_name}` (Partition: {part_id})\n"
                            f"   • *상태:* 전체 복제본 {len(replicas)}개 중 {len(isr)}개만 동기화 중\n"
                            f"   • *진단:* 브로커 유실로 인한 고가용성(HA) 경고 상태"
                        )
        # 최종 리포트 전송
        if anomaly_detected:
            error_msg = f"*[Kafka 장애 리포트]*\n" + "\n".join(diagnostics)
            print(error_msg)
            slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"{error_msg}")
        else:
            print(f"[Kafka 정상] 브로커 {active_brokers}대 가동 중, 모든 파티션 ISR 정상")
    except Exception as e:
        print(f"*[Kafka 모니터링 에러] 내부 로직 오류 또는 접속 불가: {e}")
    finally:
        await admin_client.close()
# ==========================================
# cassandra 비동기 헬스체크
# ==========================================
async def check_cassandra_ultimate_async():
    print("[Cassandra] 클러스터 상태 및 노드 진단 중...")

    # 드라이버가 동기 방식 - 별도 스레드에서 실행되도록 함수로 묶음
    def _connect_and_query():
        # 타임 아웃을 3초로 짧게 줘서, 죽었을 때 무한정 기다리지 않게 만듦
        cluster = Cluster([CASSANDRA_HOST], connect_timeout=3.0)
        try:
            session = cluster.connect()
            # Gossip 프로토콜 기반 노드 상태(Up/Down) 검사
            # 드라이버가 클러스터 전체의 메타데이터를 알고 있습니다
            down_nodes = [host.address for host in cluster.metadata.all_hosts() if not host.is_up]
            # 가벼운 쿼리로 Timeout 및 가용성(Unavailable) 테스트
            local_info = session.execute("SELECT release_version FROM system.local", timeout=2.0).one()
            version = local_info.release_version if local_info else "Unknown"

            cluster.shutdown()
            return {"status": "ok", "version": version, "down_nodes": down_nodes}

        except Exception as inner_e:
            cluster.shutdown()
            raise inner_e

    try:
        result = await asyncio.to_thread(_connect_and_query)
        # 쿼리는 성공했지만, 죽어있는 찌꺼기 노드가 발견되었을 때 (부분 장애)
        if result["down_nodes"]:
            error_msg = (
                f"*[Cassandra 클러스터 경고]*\n"
                f"- *상태:* 쿼리는 동작 중이나, 클러스터 내에 DOWN(DN) 상태인 노드가 존재합니다!\n"
                f"- *죽은 노드 IP:* `{', '.join(result['down_nodes'])}`\n"
                f"> _진단: 복제본(Replica) 유실 위험이 있습니다. `nodetool status`를 확인하고 죽은 노드를 복구하거나 제거(remove node)하세요._"
            )
            print(error_msg)
            slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
        else:
            print(f"[Cassandra 정상] 버전: {result['version']} (모든 노드 UP 상태)")
    except Unavailable as e:
        error_msg = (
            f"*[Cassandra 가용성 장애 (Unavailable)]*\n"
            f"- *요구된 일관성 수준(Consistency)을 만족할 수 없습니다.*\n"
            f"- *상세:* `{e}`\n"
            f"> _진단: 데이터를 읽고 쓰기 위해 필요한 최소 노드 수(Quorum 등)가 부족합니다. 대규모 노드 다운이 발생했습니다!_"
        )
        print(error_msg)
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
    except ReadTimeout as e:
        error_msg = (
            f"*[Cassandra 읽기 지연 (Read Timeout)]*\n"
            f"- *데이터를 제시간에 읽어오지 못했습니다.*\n"
            f"- *상세:* `{e}`\n"
            f"> _진단: 특정 노드의 디스크 I/O가 100%를 쳤거나, 툼스톤(Tombstone) 데이터가 너무 많아 쿼리가 느려졌습니다._"
        )
        print(error_msg)
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
    except WriteTimeout as e:
        error_msg = (
            f"*[Cassandra 쓰기 지연 (Write Timeout)]*\n"
            f"- *데이터 쓰기가 밀리고 있습니다.*\n"
            f"- *상세:* `{e}`\n"
            f"> _진단: 노드의 JVM GC(가비지 컬렉션) Pause가 길게 발생했거나, Commit Log 디스크가 병목을 겪고 있습니다._"
        )
        print(error_msg)
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
    except NoHostAvailable as e:
        error_msg = (
            f"*[Cassandra 접속 불가 (NoHostAvailable)]*\n"
            f"- *접속 타겟:* `{CASSANDRA_HOST}`\n"
            f"- *상세:* `{e}`\n"
            f"> _진단: 클러스터의 모든 Seed 노드가 죽었거나, 방화벽(9042 포트)이 차단되었습니다._"
        )
        print(error_msg)
        slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
    except Exception as e:
        # 알 수 없는 기타 에러 (인증 실패 등)
        print(f"[Cassandra 기타 에러] {type(e).__name__}: {e}")

async def check_postgres_ultimate_async():
    print("[PostgreSQL] 커넥션, 락, 병목 쿼리 심층 진단 중...")
    try:
        # 타임아웃을 3초로 줘서 DB가 기절했을 떄를 대비
        conn = await asyncpg.connect(PG_URL, timeout=3.0)

        diagnostics = []
        anomaly_detected = False

        # 커넥션 풀 고갈 체크 (Max 커넥션 대비 혀냊 사용량)
        current_conns = await conn.fetchval("SELECT count(*) FROM pg_stat_activity")
        max_conns_str = await conn.fetchval("SHOW max_connections")
        max_conns = int(max_conns_str)

        if current_conns >= max_conns * 0.9:
            anomaly_detected = True
            diagnostics.append(
                f"*커넥션 풀 고갈 위험*\n"
                f"   - *상태:* 사용 중 `{current_conns}` / 최대 허용 `{max_conns}`\n"
                f"   - *진단:* DB 연결이 꽉 찼습니다. 앱 서버의 Connection Pool 누수가 있거나, 트래픽 폭주로 인한 장애 직전입니다."
            )
        # 슬로우 쿼리 (메모리/CPU 병목) 체크 - 3초 이상 멈춰있는 쿼리 색출
        slow_queries = await conn.fetch('''
            SELECT pid, query, round(extract(epoch from (now() - query_start))::numeric, 1) AS duration
                FROM pg_stat_activity
                WHERE state = 'active' 
                  AND query NOT ILIKE '%pg_stat_activity%' 
                  AND now() - query_start > interval '3 seconds'
        ''')

        if slow_queries:
            anomaly_detected = True
            sq_details = [f"`[PID {q['pid']}]` {q['query'][:40]}... *(지연: {q['duration']}초)*" for q in slow_queries]
            diagnostics.append(
                f"- *병목 유발 무거운 쿼리 (Slow Query) 감지*\n   - " +
                "\n   - ".join(sq_details) +
                f"\n   - *진단:* CPU/메모리 점유율을 치솟게 만드는 악성 쿼리입니다. 인덱스가 없어서 테이블 풀 스캔(Full Scan)이 발생하고 있을 확률이 매우 높습니다!"
            )
        # 데드락 및 락 대기 경합 체크
        waiting_locks = await conn.fetchval('''
            SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'
        ''')

        if waiting_locks > 0:
            anomaly_detected = True
            diagnostics.append(
                f"*DB 락(Lock) 경합 발생*\n"
                f"   - *대기 중인 트랜잭션:* `{waiting_locks}` 개\n"
                f"   - *진단:* 누군가 특정 테이블/로우를 물고 안 놔줘서 다른 쿼리들이 줄을 서서 대기(Waiting) 중입니다. 데드락(Deadlock) 징후입니다."
            )
        # 슬랙용 최종 메시지 조립
        if anomaly_detected:
            reports_str = "\n\n".join(diagnostics)
            error_msg = (
                f"*[PostgreSQL 심층 장애 리포트]*\n"
                f"- *타겟 서버:* `localhost:5432`\n\n"
                f"*[장애 원인 분석]*\n"
                f"{reports_str}"
            )
            print(error_msg)
            slack_client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=error_msg)
        else:
            print(f"[PostgreSQL 정상] 커넥션({current_conns}/{max_conns}), 병목 쿼리 및 Lock 없음")
        await conn.close()
    except Exception as e:
        # DB가 아예 뻗어서 접속 조차 안될때
        print(f"[PostgreSQL 서버 다운] 접속 불가: {e}")

