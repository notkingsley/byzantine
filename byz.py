import logging

from server import Server


logging.basicConfig(
	level= logging.DEBUG,
	format='%(message)s',
)


def main():
	server = Server()
	server.start()


if __name__ == "__main__":
	main()