import uvicorn
from fastapi import FastAPI


app = FastAPI(title="Dummy Relay Server")

@app.post("/api/dummy/action")
async def handle_relay_action(payload: dict):
    msg_type = payload.get("type", "unknown")
    response_data = {
        "status": "success",
        "pr_status": "N/A",
        "analysis_report": "분석 보고서가 생성되지 않았습니다.",
        "type": msg_type
    }
    # APM 장애 (백엔드/DB 에러 )
    if msg_type == "apm":
        response_data["pr_status"] = "Success (PR #8291 생성됨 - dummy)"
        response_data["analysis_report"] = (
            "*[장애 원인 자동 분석 리포트]*\n"
            "- *장애 원인:* PostgreSQL `statics_raw_log` 테이블의 신규 파티션 누락으로 인한 DataIntegrityViolationException 발생.\n"
            "- *영향도:* 사용자 활동 로그 적재 불가 (해당 API 에러율 100%).\n"
            "- *자동 조치:* 누락된 파티션을 자동 생성하는 DDL 스크립트를 작성하여 긴급 복구 PR을 생성했습니다."
        )
    # Jira 이슈 (프론트/기획 버그)
    elif msg_type == "jira":
        response_data["pr_status"] = "No PR Needed (담당 팀 할당 완료 - dummy)"
        response_data["analysis_report"] = (
            "*[티켓 내용 자동 분석 리포트]*\n"
            "- *분석 결과:* UI 렌더링 시점의 상태값(State) 초기화 지연으로 인한 0점 표기 버그로 추정됩니다.\n"
            "- *의심 컴포넌트:* `MissionLevelView.tsx` 내 목표 점수 렌더링 로직.\n"
            "- *자동 조치:* 프론트엔드 파트에 티켓을 자동 할당하고, 수정이 필요한 코드 라인을 코멘트에 첨부했습니다."
        )
    return response_data

if __name__ == "__main__":
    print("⚡️ 더미 응답 서버 구동 완료 (포트 8001)")
    # 메인 서버가 8000을 쓰니까, 릴레이 서버는 8001로
    uvicorn.run(app, host="0.0.0.0", port=8001)