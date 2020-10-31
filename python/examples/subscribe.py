import pysmq
import json
import time

lastSoundTime = 0

def doFaceTime(topic, msg):
	global lastSoundTime
	face = json.loads(msg)
	if (face['joy'] > 0.9):
		if lastSoundTime + 5 < time.time():
			pysmq.publish('BLEEP', '{"cmd": "$play happy"}')
			lastSoundTime = time.time()

def main():
	pysmq.init()

	pysmq.advertise('BLEEP');
	pysmq.subscribe_hash('FACE', doFaceTime)
	pysmq.wait();

if __name__ == "__main__":
	main()