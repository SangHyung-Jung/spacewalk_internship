import requests, re, sys
import numpy as np
import os

class KAKAO_API():
    def __init__(self):
        self.api_key = os.environ['kakao_api']
        self.address_url = "https://dapi.kakao.com/v2/local/search/address.json?"
    def address_query_kakao(self, query):
        r = requests.get(self.address_url, params = {'query':query}, headers={'Authorization' : 'KakaoAK ' + self.apikey})

        try:
            aKaoonJooSo = re.split(r.json()['documents'][0]['address']['main_address_no'], query)

            return r.json()['documents'][0]['address_name'] + aKaoonJooSo[1]
        except IndexError:
            return np.nan

        except ValueError:
            try:
                return r.json()['documents'][0]['address_name']
            except:
                return np.nan

        except TypeError:
            try:
                return r.json()['documents'][0]['address_name']
            except:
                return np.nan

    def pnu_kakao(self, query, i):
        r = requests.get(self.address_url, params = {'query':query}, headers={'Authorization' : 'KakaoAK ' + self.apikey})

        try:
            address = r.json()['documents'][0]['address']
            #b_code
            b_code = address['b_code']
            #whether mountain
            if address['mountain_yn'] == 'N':
                mount = '1'
            else:
                mount = '2'
            #main_address
            main_no = address['main_address_no']
            while len(main_no) < 4:
                main_no = '0' + main_no
            #sub_address
            sub_addr = address['sub_address_no']
            while len(sub_addr) < 4:
                sub_addr = '0' + sub_addr

            pnu = b_code + mount + main_no + sub_addr

            return pnu

        except IndexError:
            return np.nan
        except TypeError:
            return np.nan

