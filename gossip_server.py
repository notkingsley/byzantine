import json
import logging
import random
from socket import socket
from threading import Event, Thread
from time import sleep
import uuid

from locked import Locked
from peer import Peer, WELL_KNOWN_PEERS
from peer_server import PeerServer


GOSSIP_INTERVAL = 60
FORWARD_AMOUNT = 3


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
			

class GossipServer(PeerServer):
	"""
	A GossipServer is a PeerServer that can join newtorks
	and stay on networks by gossiping
	"""
	def __init__(self) -> None:
		super().__init__()
		self.gossips: Locked[list[Gossip]] = Locked(list())
	

	def _start(self):
		"""
		Start the gossiper thread
		Do not call this funtion directly
		"""
		super()._start()
		self.gossip_quit = Event()
		gossiper = Thread(
			target= self.start_gossip,
			kwargs= {"quit": self.gossip_quit},
			daemon= True,
		)
		gossiper.start()
	

	def _stop(self):
		"""
		Complement of _start. Stops but does not wait for the gossiper
		Do not call directly
		"""
		super()._stop()
		self.gossip_quit.set()


	def gossip_received(self, data: dict, peers: list[Peer]):
		"""
		A gossip has been received: data["command"] == "GOSSIP"
		If required, gossip will be forwarded to a sample of peers
		"""
		gossip = Gossip(data)
		peer = None

		with self.gossips.lock:
			last = None
			for g in self.gossips.obj:
				if g.peer_name == gossip.peer_name:
					last = g
					break
			
			if not last or last and last.id != gossip.id:
				peer = self.update_peers(data)
				if peer:
					self.gossips.obj.append(gossip)
					if last:
						self.gossips.obj.remove(last)
		
		if peer:
			# never block while holding lock
			# logging.debug(f"{peer} gossiped. Updated records")
			sample = random.sample(peers, min(FORWARD_AMOUNT, len(peers)))
			with self.sock.lock:
				gossip.forward(sample, self.sock.obj)
				peer.reply_gossip(self.sock.obj, self.host, self.port, self.name)


	def gossip_reply_received(self, data: dict):
		"""
		data["command"] == "GOSSIP_REPLY"
		"""
		self.update_peers(data)


	def gossip(self, peers: list[Peer]):
		"""
		Start a gossip round to peers
		"""
		id = str(uuid.uuid4())
		with self.sock.lock:
			for peer in peers:
				peer.gossip(self.sock.obj, self.host, self.port, self.name, id)


	def start_gossip(self, quit: Event):
		"""
		Gossip forever on the network.
		Exit when quit is set
		"""
		sleep(1)
		logging.debug("Gossiping to well-known peers")
		self.gossip(WELL_KNOWN_PEERS)
		
		sleep(3)
		while True:
			peers = self.get_peers()
			# logging.debug(f"Gossiping to {len(peers)} known peers")
			self.gossip(peers)
			
			sleep(GOSSIP_INTERVAL)
			if quit.is_set():
				break