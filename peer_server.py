import logging
from threading import Event, Thread
from time import sleep, time

from base_server import BaseServer
from locked import Locked
from peer import Peer


PRUNE_INTERVAL = 45
PRUNE_TIMEOUT = 60


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
		"""
		Get all active peers
		"""
		with self.peers.lock:
			return list(self.peers.obj.values())


	def update_peers(self, data: dict) -> Peer | None:
		"""
		A new gossip or gossip reply has been received, update peers with data
		Return the associated Peer object or None if I've been whispering to myself
		"""
		if data["name"] == self.name:
			# logging.debug("Sometimes, I feel like I'm talking to myself")
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


	def cli_peers(self, sock):
		"""
		peers command received from the client over sock
		Lists all known peers
		"""
		sock.sendall(self.format_peers().encode() + b"\n")


	def find_peer(self, host, port):
		"""
		Find and return the existing Peer with address host:post
		Return None if none exists
		"""
		for peer in self.get_peers():
			if peer.host == host and peer.port == port:
				return peer
		return None
