import pandas as pd

def separate_pnu(region_code):
  aa = pd.read_excel('./shared/data/output_%s.xlsx' % region_code)
  b = []
  c = []
  d = []
  e = []
  for a in aa[1]:
    b.append(str(a)[:2])
    c.append(str(a)[:5])
    d.append(str(a)[:8])
    e.append(str(a)[:10])
  aa = aa.assign(e=b, f=c, g=d, h=e)
  sido = aa.drop_duplicates('e').e.values
  sig = aa.drop_duplicates('f').f.values
  emd = aa.drop_duplicates('g').g.values
  li = aa.drop_duplicates('h').h.values

  # 리 단위 끝자리 00 값 제거
  e = []
  for v in li:
    if v[8:10] != '00':
      e.append(v)
  li = e
  
  return {
    'sido': sido,
    'sig': sig,
    'emd': emd,
    'li': li
  }