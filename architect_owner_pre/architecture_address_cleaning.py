import pandas as pd
import numpy as np
import os, re, math
from api2 import KAKAO_API as kakaoapi
from db_helper import DB_HELPER as db_helper
from attaching_pnu import Preprocess as attaching_pnu
from tqdm import tqdm
from pykospacing import spacing
import argparse

def A_filter(tmp):
    bunji_num = '[0-9][0-4]?[0-9]?[-]?[0-9]?[0-9]?'
    sigungu = re.compile('.*시 |.*군 |.*구 ')
    dong = re.compile('.*동 |.*읍 |.*면 |.*가 |.*로 |.*길 ')
    bunji = re.compile(bunji_num)
    dongbunji = re.compile('.*동{}|.*읍{}|.*면{}|.*가{}|.*로{}|.*길{}'.format(bunji_num, bunji_num, bunji_num, bunji_num, bunji_num, bunji_num))
    for i, addr in tqdm(enumerate(tmp['final_addr']), ncols=100, ascii=True , desc="Filter A", position=0, leave=True):
        try:
            if re.search(' 산 ', addr):
                continue
            else:
                addrset = re.split(' ', addr)
                if addrset.__len__() == 1:
                    if addrset[0].__len__() >= 8: # 최소한 8글자는 존재해야 제대로된 주소 (**시 *구 *동 * -> 8글자)
                        # 띄어쓰기 안된 친구들 spacing 패키지로 띄어쓰기
                        addr = spacing(addrset[0])
                        tmp['final_addr'].iloc[i] = addr
                    else:
                        continue

                if any(element in addr for element in korea_list) == True and\
                    bool(sigungu.search(addr)) == True and ((bool(dong.search(addr)) == True and bool(bunji.search(addr)) == True) \
                    or bool(dongbunji.search(addr))== True):
                    tmp['case'].iloc[i] = 'A'
        except TypeError:
            pass # 여기도 nan data
    return tmp

def B_filter(tmp):
    C_idx_list = tmp[tmp['case'] == 'D'].index
    for idx in tqdm(C_idx_list,  ncols=100, ascii=True , desc="Filter B", position=0, leave=True):
        new_addr = kakaoapi.address_query_kakao(tmp['final_addr'].iloc[idx])
        try:
            math.isnan(new_addr)
            continue
        except:
            tmp['final_addr'].iloc[idx] = new_addr
            tmp['case'].iloc[idx] = 'B'

    return tmp

def E_filter(tmp):
    nullidx = tmp[tmp['final_addr'].isnull()].index
    for idx in tqdm(nullidx, ncols=100, ascii=True , desc="Filter C", position=0, leave=True):
        tmp['final_addr'].iloc[idx] = np.nan
        tmp['case'].iloc[idx] = 'E'

    return tmp

def nospace(addr):
    try:
        if re.match(' ', addr):
            addr = re.sub(re.match(' ', addr).group(), '', addr, 1)
        else:
            addr = addr
        if 'nan' in addr:
            addr = re.sub('nan', '', addr)
        else:
            addr = addr

        if '  ' in addr:
            addr = re.sub('  ', ' ', addr)
        else:
            addr = addr
    except TypeError:
        pass # nan 데이터

    return addr

def cleaning_owner_address(tmp):

    # 순서 1
    tmp['final_addr'] = tmp['loc_detl_addr'].copy()
    ### 모든 case는 C로부터 시작
    # 순서 2
    tmp['case'] = 'D'
    # 순서 3
    for i in tqdm(range(len(tmp)), ncols=100, ascii=True , desc="Making_final_addr", position=0, leave=True):
        # case A-1 : 멀쩡한 애들 ( 코드 상으로 안건드림 )
        # case A-3 : 왼쪽으로 나와있는 경우
        if tmp['loc_sigungu_nm'].iloc[i] != ' ':
            sig_cd = tmp['loc_sigungu_cd'].iloc[i]
            bjd_cd = tmp['loc_bjdong_cd'].iloc[i]
            pnu = sig_cd + bjd_cd
            ad_num = tmp['final_addr'].iloc[i]

            ### 예외처리 ( 동대문구에서 pnu가 '      ' 이런 경우가 있었고, ad_num이 빈칸인 경우가 있었음 )
            if '  ' in pnu:
                continue
            try:
                refreshed = region_cd[region_cd['bjd_cd'] == pnu]['region_nm'].values[0] + ' ' + ad_num
                tmp['final_addr'].iloc[i] = refreshed
            except TypeError:
                if math.isnan(ad_num) == True:
                    ad_num = ''
            
        # case A-3 : 오른쪽으로 튀어나와 있는 경우 ( )
        if tmp['na_loc_plat_plc'].iloc[i] != ' ':
            refreshed = tmp['na_loc_plat_plc'].iloc[i] + str(tmp['na_loc_detl_addr'].iloc[i])

            tmp['final_addr'].iloc[i] = refreshed
    #순서 4, 5
    # nan, '  '이 포함되어 있는 row가 존재하는 데 ( 많지는 않으나 ) 필터를 거치기 전에 지워줘야 최대한 많이 살릴 수 있음
    # 순서 5
    tmp['final_addr'] = tmp['final_addr'].apply(nospace)
    # 순서 6
    tmp = A_filter(tmp)
    # 순서 7
    # 순서 8
    tmp = B_filter(tmp)
    tmp = E_filter(tmp)

    # 숫자로 시작하는 A 주소가 존재 ( 143-1 땡땡빌라 몇동 몇호 - 최후로 카카오 검색 해보고 안되면 D )

    return tmp

if __name__ == "__main__":
    #순서1
    j = 0
    api_usage = 0
    parser = argparse.ArgumentParser()
    parser.add_argument('X', type=int,
                help="지역 번호를 적으시오 (숫자만) ( Ex. 서울 = 11, 인천 = 28 , 경기 = 41 )")
    args = parser.parse_args()
    place = args.X
    #순서2
    gu_list = os.listdir('C:\\Users\\user\\Desktop\\ZICBBANG\\owner_per_gu_{}'.format(place))
    #순서3
    region_cd = db_helper.read_tables(dbname = 'spwkdw', table_name = 'bjdong_code')
    korea_cd = [cd for cd in region_cd['bjd_cd'] if '00000000' in cd]
    korea_list = [region_cd[region_cd['bjd_cd'] == cd]['region_nm'].values[0] for cd in korea_cd]

    for region in korea_list:
        if len(region) == 5:
            korea_list.append(region[:2])
        if len(region) == 3:
            korea_list.append(region[:2])
        if len(region) == 4:
            korea_list.append(region[0]+region[2])
    #순서4
    for i in gu_list:
        gu = i[10:15]
        print('-'*50)
        print('-'*50)
        print(region_cd[region_cd['bjd_cd']==gu+'00000']['region_nm'].values, '시작합니다')
        owner = pd.read_csv('C:\\Users\\user\\Desktop\\ZICBBANG\\owner_per_gu_{}\\owner_res_{}.csv'.format(place, gu), encoding='EUC_KR')

        clean_owner = cleaning_owner_address(owner)
    #순서5
        clean_owner = attaching_pnu.match_pnu_with_addr(clean_owner, place)
        api_usage += clean_owner['matched_pnu'].isnull().sum()
    #순서6
        if api_usage > 24000:
            api_usage = 0
            j += 1
        clean_owner.to_csv("C:\\Users\\user\\Desktop\\test_{}.csv".format(gu), encoding="EUC-KR", index=False)
        print("저장완료")
    #순서7
        # apply 함수를 쓰지 않으면 됨. 이 아니라, 그냥 처음부터 indexing을 할 때 A, B만 하면 되겠네 이게 되나?
        for i, addr in tqdm(enumerate(clean_owner['final_addr']), ncols=100, ascii=True , desc="C_filter", position=0, leave=True):
            try:
                if ((clean_owner['case'].iloc[i] == 'A') | (clean_owner['case'].iloc[i] == 'B')) & (math.isnan(clean_owner['matched_pnu'].iloc[i])):
                    clean_owner['matched_pnu'].iloc[i] = kakaoapi.pnu_kakao(addr, j)
                    api_usage += 1
                    if math.isnan(clean_owner['matched_pnu'].iloc[i]):
                        clean_owner['case'].iloc[i] = 'C'
                    else:
                        pass
            except TypeError:
                continue
    #순서8
        clean_owner['matched_pnu'] = clean_owner['matched_pnu'].fillna(0)
        clean_owner['matched_pnu'] = clean_owner['matched_pnu'].astype('int64')
        print(api_usage)
        print("-"*20)
        print('A 등급 : {} / {} '.format(len(clean_owner[clean_owner['case']=='A']), len(clean_owner)))
        print('B 등급 : {} / {} '.format(len(clean_owner[clean_owner['case']=='B']), len(clean_owner)))
        print('C 등급 : {} / {} '.format(len(clean_owner[clean_owner['case']=='C']), len(clean_owner)))
        print('D 등급 : {} / {} '.format(len(clean_owner[clean_owner['case']=='D']), len(clean_owner)))
        print('E 등급 : {} / {} '.format(len(clean_owner[clean_owner['case']=='E']), len(clean_owner)))

        clean_owner.to_csv("C:\\Users\\user\\Desktop\\py\\owner_per_gu_{}\\owner_res_{}_final.csv".format(place, gu), encoding = "EUC-KR", index=False)
