#New_law_checker
### 서울시, 경기도(군제외) 법령들 갱신여부 체커 만들 (갱신확인모드가 디폴트) (엑셀형태로 같은 디렉토리에 저장)

#임포트
import requests #웹을 읽을 때
from bs4 import BeautifulSoup as bs #웹에서 크롤링 할때
import os #현재 운영체제(디렉토리) 다룰때
import datetime
import psycopg2
import sys
import numpy as np
import pandas as pd
sys.path.append("..")
os.getcwd()
from shared.helper import slackbot_helper
from shared.helper import database_helper

# table = "newlaw_check_tbl"
# current_dir = 'www_law_go_kr'
d = datetime.date.today()

def job():
  #순서 1
  table = 'newlaw_check_tbl'

  law_trash_can =[] # 불일치하는 법령들 리스트

  ## 전국 건축관련 법령 ;
  #순서 2
  Law_keys_1 = ['국토의 계획 및 이용에 관한 법률', '건축법', '주택법', '개발제한구역의 지정 및 관리에 관한 특별조치법', \
    '도시공원 및 녹지 등에 관한 법률', '주차장법', '건축물의 피난ㆍ방화구조 등의 기준에 관한 규칙', \
      '장애인ㆍ노인ㆍ임산부 등의 편의증진 보장에 관한 법률']
  law_tbl_1 = []

  #순서 3
  for x in range(0, len(Law_keys_1)) : #법규 별로 반복
    #검색 데이터
    # 순서 4
    Input_data = {
      #자치법규 외에 법령 다룰때 코드
      'q': Law_keys_1[x],
      'outmax' : '50',
      'p2' : '3',
      'pg' : '1',
      'fsort': '10,21',
      "lsType": '8',
      'section' : 'lawNm',
      'lsiSeq': '0'
    }
    ##url 인포 읽기
    request_url = 'http://www.law.go.kr/lsScListR.do' #개발자도구 네트워크 lsScListR.do에서 얻은 세션

    ##세션 읽기
    with requests.Session() as s:
      req = s.get(request_url)
      is_ok = req.ok
      if(is_ok) : print("Law connected")

    r = s.post(request_url, cookies=s.cookies, data=Input_data) #url 쿠키읽기
    soup = bs(r.text, 'html.parser')
    #순서 5
    for y in range(0, len(soup.find_all('td', {'class' : 'tl'}))) : #법규 중에 법규 종류별로 반복
      # y=0
      #순서 6
      lawtitle = soup.find_all('td', {'class' : 'tl'})[y].text.strip() #공백제거
      if lawtitle[0:len(Law_keys_1[x])] != Law_keys_1[x]:
        law_trash_can.append(lawtitle) #읽어낸 법규가 검색한 법규명과 다르면 law_trash_can에 추가
      else :
        tbody_tag = soup.tbody # 테이블 body추출
        law_profile = tbody_tag.contents[2*y+1] #contents 함수 사용시, \n 이라는 배열이 추가출력돼서 홀수 주소로 입력.
        # 최신 법규 개요 불러오기
        law_nm = law_profile.find('td', {'class' : 'tl'}).text.strip() #법규명
        enf_dt = law_profile.find_all('td')[2].text #시행일자
        law_type = law_profile.find_all('td')[3].text # 법령종류
        pub_no = law_profile.find_all('td')[4].text #공포번호
        pub_dt = law_profile.find_all('td')[5].text #공포일자
        redev_type = law_profile.find_all('td')[6].text # 재개정 구분
        url = 'http://www.law.go.kr/lsSc.do?tabMenuId=tab54&query='+ law_nm.replace(' ', '%20') #법률 관련 url

        #순서 7
        # workspace 데이터베이스에 접속해서 공포번호 비교
        #================== Database Connection =================== start
        pg_local = {
                    'host' : os.environ['AWS_HOST'],
                    'user' : os.environ['AWS_USER'],
                    'password' : os.environ['AWS_PW']
                }

        conn_string = "host={host} user={user} password = {password}"\
            .format(**pg_local) + " dbname = workspace"
        # conn_string = 'host=spwk-dw.cicvuwhjlhxo.ap-northeast-2.rds.amazonaws.com port=5432 dbname=workspace user=root password=!SZ41+2P*h'
        try:
          conn = psycopg2.connect(conn_string)
        except psycopg2.Error as e:
          print(datetime.datetime.now(), 'Error database connection!', e)
        else:
          print(datetime.datetime.now(), 'Database workspace connected!')
        curs = conn.cursor()
        #================== Database Connection =================== end
        try:
          curs.execute('SELECT pub_no FROM %s WHERE law_nm = \'%s\' ;' % (table, law_nm)) #sql문으로 읽고 fetchall하면 읽힘
          tmp=curs.fetchall()[0][0] #원래 db에 적혀있는 해당 법규의 공포번호
          if pub_no > tmp :#방금 크롤링한 공포번호가 더 크면 알림을 보낸다.
            slackbot_helper.slack_message('#신규법규_알람봇', law_nm + ' `갱신됨`' + "\n 공포일자: " + pub_dt + " | 시행일자: " + enf_dt + " | 재개정 구분: " + redev_type + "\n URL: " + url)
            is_renewal = 'True'
          else:
            is_renewal = ''
        except: is_renewal = ''
        #순서 8
        # 크롤링한걸 쌓아두되 알림보낸 법규만 is_renewal 컬럼에 True값으로 추가한다.
        law_tbl_1.append([
          law_type,
          law_nm,
          enf_dt,
          pub_no,
          pub_dt,
          redev_type,
          is_renewal,
          datetime.datetime.now(),
          url
        ])
        current_rows = len(law_tbl_1)

  #순서 9
  ## 서울/경기 도시계획조례 ; '초기'라는 키워드가 들어간 코드는 처음 데이터 제작할 때만 이용하므로 그 후엔 주석처리해서 스크립트 이용

  Law_keys_2 = ['도시계획 조례', '건축 조례']
  # 순서 10
  SiDo_Keys = [6110000, 6410000] #서울, 경기
  # 순서 11
  Cities_keys = [6110000, \
    3740000, 3780000, 3820000, 3830000, 3860000,3900000, 3910000, 3920000, 3930000, 3940000, 3970000, 3980000, 3990000,\
    4000000, 4010000, 4020000, 4030000, 4040000, 4050000, 4060000, 4070000, 4080000, 4090000,\
    5530000, 5540000,5590000, 5600000, 5700000]
  law_tbl_2 = []

  for z in range(0, len(Law_keys_2)) :
    for i in range(0,len(Cities_keys)) :
      if Cities_keys[i] == 6110000 :
        Si_index = 0
      else :
        Si_index = 1

      #검색 데이터
      # if (Law_keys_2[z] == '도시계획 조례') & (Cities_keys[i] == 4060000):
      #   break
      # 순서 12
      Input_data = {
        'q': Law_keys_2[z],
        'outmax' : '15',
        'p1' : Cities_keys[i], # 시군구 키 입력
        'p2' : '30001',
        'p3': '2,3',
        'p7': SiDo_Keys[Si_index], #시도 키 입력
        'idxList': 'LsKwdNm_idx,OrdinNm_idx',
        'pg' : '1',
        'section' : 'ordinNm',
        'dtlYn': 'N'
      }

      ##url 인포 읽기
      request_url = 'http://www.law.go.kr/ordinScListR.do' #개발자도구 네트워크 ordinScListR.do에서 얻은 세션

      ##세션 읽기
      with requests.Session() as s:
        req = s.get(request_url)
        is_ok = req.ok
        if(is_ok) : print("Law connected")

      r = s.post(request_url, cookies=s.cookies, data=Input_data) #url 쿠키읽기
      soup = bs(r.text, 'html.parser')

      ## 최신자치법규 개요 추출
      law_num = 0
      for j in range(0, len(soup.find_all('td', {'class' : 'leco'}))) :
        lawtitle = soup.find_all('td', {'class' : 'leco'})[j].text.strip()
        if lawtitle[-len(Law_keys_2[z]):] == Law_keys_2[z] or lawtitle[-len(Law_keys_2[z])+1:] == Law_keys_2[z].replace(" ","") : #법규이름에 따라 글자 수 다르게(이상하게 3칸씩 당겨져있음)
          if law_num == 1:
            break
          else: law_num += 1
          tbody_tag = soup.tbody # 테이블 body추출
          law_profile = tbody_tag.contents[2*j+1] #contents 함수 사용시, \n 이라는 배열이 추가출력돼서 홀수 주소로 입력.
          # '도시계획조례' 최신 법규 개요 불러오기
          law_nm = law_profile.find('td', {'class' : 'leco'}).text.strip() #법규명
          enf_dt =''
          law_type = law_profile.find_all('td', {'class' : 'ce'})[2].text # 법령종류
          pub_no = law_profile.find_all('td', {'class' : 'ce'})[3].text #공포번호
          pub_dt = law_profile.find_all('td', {'class' : 'ce'})[4].text #공포일자
          redev_type = law_profile.find_all('td', {'class' : 'ce'})[5].text # 재개정 구분
          url = 'http://www.law.go.kr/ordinScNw.do?tabMenuId=tab160&query='+ law_nm.replace(' ', '%20') #법률 관련 url

          # print(law_nm)
          # workspace 데이터베이스에 접속해서 공포번호 비교
          #================== Database Connection =================== start
          pg_local = {
                      'host' : os.environ['AWS_HOST'],
                      'user' : os.environ['AWS_USER'],
                      'password' : os.environ['AWS_PW']
                  }
          conn_string = "host={host} user={user} password = {password}"\
              .format(**pg_local) + " dbname = workspace"
          # conn_string = 'host=spwk-dw.cicvuwhjlhxo.ap-northeast-2.rds.amazonaws.com port=5432 dbname=workspace user=root password=!SZ41+2P*h'
          try:
            conn = psycopg2.connect(conn_string)
          except psycopg2.Error as e:
            print(datetime.datetime.now(), 'Error database connection!', e)
          else:
            print(datetime.datetime.now(), 'Database workspace connected!')

          curs = conn.cursor()
          #================== Database Connection =================== end
          try:
            curs.execute('SELECT pub_no FROM %s WHERE law_nm = \'%s\' ;' % (table, law_nm)) #sql문으로 읽고 fetchall하면 읽힘
            tmp=curs.fetchall()[0][0] #원래 db에 적혀있는 해당 법규의 공포번호

            if pub_no > tmp :#방금 크롤링한 공포번호가 더 크면 알림을 보낸다.
              slackbot_helper.slack_message('#신규법규_알람봇', law_nm + ' `갱신됨`' + "\n 공포일자: " + pub_dt + " | 재개정 구분: " + redev_type + "\n URL: " + url)
              is_renewal = 'True'
            else: is_renewal = ''
          except: is_renewal = ''

          # 크롤링한걸 쌓아두되 알림보낸 법규만 is_renewal 컬럼에 True값으로 추가한다.
          law_tbl_2.append([
            law_type,
            law_nm,
            enf_dt,
            pub_no,
            pub_dt,
            redev_type,
            is_renewal,
            datetime.datetime.now(),
            url
          ])
          current_rows = len(law_tbl_2)
        else :
          law_trash_can.append(lawtitle)

  print(current_rows)
  # law_tbl_1 리스트를 데이터베이스에 넣기
  #순서 13
  if pd.DataFrame(law_tbl_1)[6].isin(['True']).sum() +\
     pd.DataFrame(law_tbl_2)[6].isin(['True']).sum() == 0:
    slackbot_helper.slack_message('#신규법규_알람봇', '오늘 법규 날씨는 맑습니다. 안심하세요.')
  else:
    slackbot_helper.slack_message('#신규법규_알람봇', '오늘 법규 날씨는 흐리네요. 건승하세요.')
  #순서 14
  current_dir = 'www_law_go_kr'
  #순서 15
  table = 'new_law_test'
  #순서 16
  database_helper.drop_create_table(conn, curs, table, current_dir)
  database_helper.insert_array(conn, curs, table, current_rows, law_tbl_1, log=False)
  database_helper.insert_array(conn, curs, table, current_rows, law_tbl_2, log=False)
  # if d.weekday() == 6 : # 일요일에는 크롤러 생존확인 및 스티브잡스의 말씀(열람공고 크롤러에서 이어짐)
  #   slackbot_helper.slack_message('#신규법규_알람봇', 'Crwalers are working...')

if __name__ == "__main__":
  job()
