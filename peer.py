import json
from socket import socket
from time import time


class Peer:
	"""
	Represent a self conscious peer
	"""
	def __init__(self, name: str, host: str, port: int) -> None:
		self.name = name
		self.host = host
		self.port = int(port) # don't trust peers to not send str as port
		self.last = time()
		self.word: str = None
	

	def __str__(self):
		return f"<Peer name={self.name}, host={self.host}, port={self.port}>"
	

	def __repr__(self) -> str:
		return str(self)
	

	def send(self, sock: socket, data: bytes):
		"""
		Send data to this peer through socket sock
		"""	
		sock.sendto(data, (self.host, self.port))
	

	def reply_gossip(self, sock: socket, host, port, name) -> None:
		"""
		Compose and send a GOSSIP_REPLY message to this peer
		Use host, port and name as current node's info
		"""
		self.send(sock, json.dumps(
			{
				"command": "GOSSIP_REPLY",
				"host": host,
				"port": port,
				"name": name,
			}
		).encode())
	

	def gossip(self, sock: socket, host, port, name, id) -> None:
		"""
		Compose and send a new GOSSIP to this peer
		Use host, port and name as current node's info
		"""
		self.send(sock, json.dumps(
			{
				"command": "GOSSIP",
				"host": host,
				"port": port,
				"name": name,
				"messageID": id,
			}
		).encode())


WELL_KNOWN_PEERS = [
	Peer("well-known 1", "localhost", 8411),
	Peer("well-known 2", "localhost", 8412),
	Peer("well-known 3", "localhost", 8413),
]