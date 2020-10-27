import pysmq

def main():
	pysmq.init()

	pysmq.advertise_hash('MARC');
	pysmq.spin_once();
	pysmq.spin_once();
	pysmq.publish_hash('MARC', '{"cmd": "$123"}')
	pysmq.spin_once();
	pysmq.spin_once();

if __name__ == "__main__":
	main()