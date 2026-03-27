# 목적

# 초기 세팅

.env에 Jira, Slack 설정 후 값 추가..(작성중)

```
# 가상환경 생성

python3 -m venv [환경이름]

# 가상환경 활성화
source ./환경이름/bin/activate

# 가상환경에 패키지 설치 
pip intall -r requirements
```
# ngrok 
로컬에서 서버 띄웠을 때 외부에서 접속 가능하게 포워딩 해줌
https://dashboard.ngrok.com/get-started/setup/linux
```
# ngrok 설치 후 실행 포트로 포워딩 
ngrok http 8000
```