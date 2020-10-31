import pysmq

def main():
	pysmq.init()

	pysmq.advertise_hash('MARC');
	pysmq.wait_for(10000)
	pysmq.publish_hash('MARC', '{"cmd": "$815"}')
	pysmq.wait_for(100)

if __name__ == "__main__":
	main()
