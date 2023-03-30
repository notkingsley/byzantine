import json
from socket import socket

from peer import Peer


class Gossip:
	"""
	Some gossip we've recently heard
	"""
	def __init__(self, data: dict):
		self.id = data["messageID"]
		self.peer_name = data["name"]
		self.data = data
	

	def __str__(self):
		return f"<Gossip from={self.peer_name}, id={self.id}>"
	

	def __repr__(self) -> str:
		return str(self)
	

	def forward(self, peers: list[Peer], sock: socket) -> None:
		"""
		Forward gossip to peers over sock
		"""
		# logging.debug(f"Forwarding {self} to {peers}")
		data = json.dumps(self.data).encode()
		for peer in peers:
			peer.send(sock, data)