from slacker import Slacker
import os

## 슬랙채널 이름을 적고 메시지 내용을 적음; 채널에 대한 토큰확보 후 스크립트를 추가해 줘야함. ex) post_slack_message(#newlaw_bot, "안녕?")
def slack_message(channel, message):
  try:
    token = os.envrion['slack_token']
    slack = Slacker(token)
    slack.chat.post_message(channel, message, as_user=True)
    print("The message is posted to " + channel)
  except:
    print("We cannot connect the slack because of the empty token")

