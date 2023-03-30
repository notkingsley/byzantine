import logging
import random
from threading import Event, Thread
from time import sleep, time
import uuid

from base_server import BaseServer
from gossip import Gossip
from locked import Locked
from peer import Peer, WELL_KNOWN_PEERS


GOSSIP_INTERVAL = 10
PRUNE_INTERVAL = 15
PRUNE_TIMEOUT = 20
FORWARD_AMOUNT = 3


class PeerServer(BaseServer):
	"""
	A PeerServer allows us to manage our peers
	"""
	def __init__(self) -> None:
		super().__init__()
		self.peers: Locked[dict[str: Peer]] = Locked(dict())
	

	def _start(self):
		"""
		Start the gardener thread.
		Do not call directly.
		"""
		super()._start()
		self.gardener_quit = Event()
		gardener = Thread(
			target= self.prune_peers,
			kwargs= {"quit": self.gardener_quit},
			daemon= True,
		)
		gardener.start()
	

	def _stop(self):
		"""
		Complement of _start, stops the gardener
		Do not call direcly
		"""
		super()._stop()
		self.gardener_quit.set()


	def get_peers(self) -> list[Peer]:
		with self.peers.lock:
			return list(self.peers.obj.values())


	def update_peers(self, data: dict) -> Peer | None:
		"""
		A new gossip or gossip reply has been received, update peers with data
		Return the associated Peer object or None if I've been whispering to myself
		"""
		if data["name"] == self.name:
			logging.debug("Sometimes, I feel like I'm talking to myself")
			return None

		with self.peers.lock:
			peer = self.peers.obj.get(data["name"])
			if not peer:
				peer = Peer(data["name"], data["host"], data["port"])
				self.peers.obj[data["name"]] = peer
			else:
				peer.last = time()

			return peer
	

	def prune_peers(self, quit: Event):
		"""
		Occassionally remove peers that haven't gossiped in a while
		We don't prune gossips and get one hanging gossip per dead peer
		Exit when quit is set
		"""
		while True:
			with self.peers.lock:
				stale_peers: list[str] = list()

				for name in self.peers.obj.keys():
					if time() - self.peers.obj[name].last > PRUNE_TIMEOUT:
						stale_peers.append(name)
				
				if stale_peers:
					logging.debug(f"Kicking peers: {stale_peers}")
				for name in stale_peers:
					self.peers.obj.pop(name)

			sleep(PRUNE_INTERVAL)
			if quit.is_set():
				break


	def format_peers(self):
		"""
		Return all peers and the last word received from each, 
		formatted into a string
		"""
		f = lambda peer: f"{peer}, Last word: {peer.word}"
		with self.peers.lock:
			return f"{[f(peer) for peer in self.peers.obj.values()]}"


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
		logging.debug("Starting gossiper")
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
		logging.debug("Stopping gossiper")
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
			logging.debug(f"{peer} gossiped. Updated records")
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
			logging.debug(f"Gossiping to {len(peers)} known peers")
			self.gossip(peers)
			
			sleep(GOSSIP_INTERVAL)
			if quit.is_set():
				break