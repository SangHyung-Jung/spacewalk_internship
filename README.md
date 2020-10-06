# spacewalk_internship

2020.01 ~ 2020.02 인턴 생활 중 수행했던 업무 py 파일 정리

## architect_owner_pre
### api2.py
카카오 지도 api 연결, pnu 값 추출하는 모듈
### attaching_pnu.py
기존 DB에의 pnu와 주소 값을 매칭하는 모듈
### db_helper.py
사내 DB 연결 ( append, alter, mk, read )
### architect_address_cleaning.py
main.py ( 총 5개의 필터를 사용해서 건축물 소유자 주소를 정제, 사내 DB에 구축 )

## new_law_checker
### shared
각종 helper py
### sql
sql 명령어
### main.py
매일 아침 09시, 건축 관련 법규가 수정되었을 시, Slack 봇의 알림과 함께 DB 최신화
