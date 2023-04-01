from collections import Counter
import json
import logging
from socket import socket
from threading import Event, Thread
from time import time
import uuid

from db_server import DBServer
from locked import Locked
from peer import Peer


CONSENSUS_TIME = 10
CONSENSUS_WAIT_FOR = 0.8


class Consensus:
	"""
	A Consensus object represents a live consensus
	"""
	def __init__(
		self,
		OM: int,
		index: int,
		value: str,
		received: str,
		peers: list[Peer],
		id: str,
		due: float
	):
		self.received = received
		self.peers = peers
		self.due = due
		self.msg = json.dumps(
			{
				"command": "CONSENSUS",
				"OM": OM,
				"index": index,
				"value": value,
				"peers": [f"{peer.host}:{peer.port}" for peer in self.peers],
				"messageID": id,
				"due": self.due,
			}
		).encode()
		self.replies: Locked[Counter[str]] = Locked(Counter())
		self.done_waiting = Event()


	def execute(self, sock: Locked[socket]):
		"""
		Execute the consensus
		This performs the (sub-)consensus and waits for it
		Return the word according to the majority
		"""
		with sock.lock:
			for peer in self.peers:
				peer.send(sock.obj, self.msg)
		
		if self.done_waiting.wait((self.due - time()) * CONSENSUS_WAIT_FOR):
			# logging.debug("Everyone replied!")
			pass
		else:
			# logging.debug("Using incomplete consensus results")
			pass
		
		with self.replies.lock:
			# logging.debug(f"Consensus results: {list(self.replies.obj.elements())}")
			if not self.replies.obj.total():
				logging.error("Nobody replied :(")

			self.replies.obj[self.received] += 1
			return self.replies.obj.most_common(1)[0][0]
		

	def notify(self, value: str):
		"""
		Notify this consensus of a reply that matched it's id
		This always runs in the separate thread of a handle_message
		"""
		with self.replies.lock:
			self.replies.obj[value] += 1

			if self.replies.obj.total() == len(self.peers):
				self.done_waiting.set()


class ConsensusServer(DBServer):
	"""
	Implements functionalities that lets us organize
	and participate in a consensus
	"""
	def __init__(self) -> None:
		super().__init__()
		self.consensuses: Locked[dict[str: Consensus]] = Locked(dict())
	

	def do_consensus(
		self,
		OM: int,
		index: int,
		value: str,
		received: str,
		peers: list[Peer],
		due: float
	):
		"""
		Run a (sub-)consensus with given parameters.
		Update db and return the correct majority value
		"""
		id = str(uuid.uuid4())
		consensus = Consensus(OM, index, value, received, peers, id, due)
		with self.consensuses.lock:
			self.consensuses.obj[id] = consensus

		self.db.set(index, consensus.execute(self.sock))
		with self.consensuses.lock:
			self.consensuses.obj.pop(id)

		return self.db.get_no_lie(index)


	def start_consensus(self, index: int):
		"""
		Begin a new consensus on index index
		Update db and return the word according to consensus
		"""
		return self.do_consensus(
			self.determine_OM(),
			index,
			self.db.get(index),
			None,
			self.get_peers(),
			time() + CONSENSUS_TIME
		)
	

	def determine_OM(self):
		"""
		Determine and return the maximum OM level from the number of known peers
		"""
		with self.peers.lock:
			return int(len(self.peers.obj) / 3)
	

	def consensus_received(self, data: dict, addr):
		"""
		A CONSENSUS command was received; data["command"] == "CONSENSUS"
		We don't actually run the sub-consensus if we're lying
		"""
		# logging.debug(f"Got consensus with OM({data['OM']})")
		if not data["OM"] or self.db.lying:
			word = self.db.get(data["index"])

		else:
			word = self.do_consensus(
				data["OM"] - 1,
				data["index"],
				self.db.get(data["index"]),
				data["value"],
				[Peer("", *peer.split(":")) for peer in data["peers"]],
				data["due"],
			)

		reply = json.dumps(
			{
				"command": "CONSENSUS-REPLY",
				"value": word,
				"reply-to": data["messageID"],
			}
		).encode()
		# logging.debug(f"Sending {reply} to {addr}")

		with self.sock.lock:
			self.sock.obj.sendto(reply, addr)
	

	def consensus_reply_received(self, data: dict, addr):
		"""
		data["command"] == "CONSENSUS-REPLY"
		"""
		with self.consensuses.lock:
			c: Consensus = self.consensuses.obj.get(data["reply-to"])
		
		if c:
			c.notify(data["value"])
		
		# might not work due to subtle differences 
		# ("127.0.0.1", "0.0.0.0", "127.0.1.1") etc
		# therefore last words never get updated
		peer = self.find_peer(*addr)
		if peer:
			peer.word = data["value"]
	

	def cli_consensus(self, sock: socket, index: str):
		"""
		consensus x received from CLI client
		"""
		index = int(index)
		sock.sendall(
			f"Running consensus on index {index}. Give it a minute..\n".encode()
		)
		spawn = Thread(
			target= self.spawn_consensus,
			kwargs= {"sock": sock, "index": index},
			daemon= True,
		)
		spawn.start()


	def spawn_consensus(self, sock: socket, index: int):
		"""
		Call start_consensus in a separate thread since 
		we don't want to block the main thread
		"""
		word = self.start_consensus(index)
		sock.sendall(
			f"\nConsensus done!\nWord at index {index} is {word}\n>>> ".encode()
		)