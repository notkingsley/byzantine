import json
import logging
import selectors
from threading import Thread

from cli_server import CLIServer
from gossip_server import GossipServer


class Server(GossipServer, CLIServer):
	"""
	A Server combines all the features from base server classes
	and does the listening and selecting
	"""
	def __init__(self) -> None:
		super().__init__()
		self.selector = selectors.DefaultSelector()
		self.selector.register(self.cli_server, selectors.EVENT_READ)
		self.selector.register(self.sock.obj, selectors.EVENT_READ)


	def handle_message(self, data: bytes, addr):
		"""
		Accept a new message and digest it in a separate thread
		We assume data is valid. If anything goes wrong, thread crashes 
		and server's good as if the message was never received
		"""
		data = json.loads(data)
		# logging.debug(f"Got {data} from {addr}")
		command = data["command"]
		
		if command == "GOSSIP":
			self.gossip_received(data, self.get_peers())
		
		elif command == "GOSSIP_REPLY":
			self.gossip_reply_received(data)
		
		else:
			logging.debug(f"Invalid command received: {command}")
	

	def start(self):
		try:
			super()._start()
			while True:
				events = self.selector.select()

				for event, _ in events:
					if event.fileobj == self.sock.obj:
						with self.sock.lock:
							data, addr = self.sock.obj.recvfrom(4096)
							handler = Thread(
								target= self.handle_message,
								kwargs= {"data": data, "addr": addr},
								daemon= True,
							)
							handler.start()

					elif event.fileobj == self.cli_server:
						connection, client = self.cli_server.accept()
						connection.setblocking(False)
						logging.debug(f"Client {client} connected.")
						connection.sendall(b"Hola!! You know what to do.\n")
						self.selector.register(connection, selectors.EVENT_READ)

					else:
						data: bytes = event.fileobj.recv(4096)
						# logging.debug(f"Got {data}")
						self.dispatch(event.fileobj, *data.decode().strip().split())
					
		finally:
			super()._stop()