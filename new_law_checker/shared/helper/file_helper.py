import boto3
import os
import datetime
import time
import zipfile
import unicodedata

# 해당 경로에 폴더가 없으면 생성 (경로)
def make_folder_not_exist(path):
  if not os.path.exists(path):
    os.makedirs(path)

# 디렉토리 내부에 해당 확장자가 있는지 체크 (디렉토리 경로, 확장자)
def fileExt_exist_checker(check_dir, ext):
  for file in os.listdir(check_dir):
    print(file)
    if file.endswith(ext):
      return True

# 디렉토리 내부에 해당 파일 있는지 체크 (디렉토리 경로, 파일)
def file_exist_checker(check_dir, filename):
  for file in os.listdir(check_dir):
    if file == filename:
      return True

def files_indir_make_zip(source_dir, file_nm):
  with zipfile.ZipFile('%s%s.zip' % (source_dir, file_nm), mode='w') as myzip:
    for file in os.listdir(source_dir):
      if not file.endswith('.zip'):
        myzip.write(unicodedata.normalize('NFC', '%s%s' % (source_dir, file)))

# AWS S3에 소스디렉토리 내 파일들 업로드 (원본 디렉토리, S3 디렉토리, 압축 파일 이름, 확장자)
def files_indir_upload_to_s3(source_dir, target_dir, file_nm, extra_args=None):
  region = 'https://s3.ap-northeast-2.amazonaws.com'
  bucket = os.environ['bucket']
  s3client = boto3.client('s3')
  uploaded = []
  file_nm = unicodedata.normalize('NFC', file_nm)
  files_indir_make_zip(source_dir, file_nm)

  while not file_exist_checker(source_dir, '%s.zip' % file_nm):
    time.sleep(3)
    
  for file in os.listdir(source_dir):
    if file == '%s.zip' % file_nm:
      print(datetime.datetime.now(), 'Uploading %s to S3... ' % file)
      s3client.upload_file('%s/%s' % (source_dir, file), bucket, '%s/%s' % (target_dir, file), ExtraArgs=extra_args)
      uploaded.append('%s/%s/%s/%s' % (region, bucket, target_dir, file_nm + '.zip'))

  return uploaded
