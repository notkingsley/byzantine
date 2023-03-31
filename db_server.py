import json
import logging
from queue import Empty, Queue
import random
from socket import socket
from threading import Thread
from time import sleep

from locked import Locked
from gossip_server import PeerServer


DB_SIZE = 5


class Database:
	"""
	Maintains a database of DB_SIZE values
	Uses Locked, so essentially threadsafe.
	Also abstracts lying
	"""
	def __init__(self) -> None:
		self._db = Locked([None] * DB_SIZE)
		self.lying = False
	

	def get(self, index: int) -> str:
		"""
		Return the word at index. 
		A wrong word is returned if we're lying
		"""
		with self._db.lock:
			word = self._db.obj[index]
		if self.lying:
			word += " lie"
		return word
	

	def set(self, index: int, word: str):
		"""
		Set the word at index to word
		Unaffected by lying
		"""
		with self._db.lock:
			self._db.obj[index] = word
	

	def all(self):
		"""
		Return a copy of the database
		Unaffected by lying
		"""
		with self._db.lock:
			return self._db.obj[:]
	

	def reset(self, db: list[str | None]):
		"""
		Reset database object to db. Not checked for errors
		"""
		with self._db.lock:
			self._db.obj = db
	

	def lie(self):
		"""
		Start lying. Words returned will be incorect
		"""
		self.lying = True
	

	def truth(self):
		"""
		Stop lying. Always return honest answers
		"""
		self.lying = False


class DBServer(PeerServer):
	"""
	A DBServer implements functionalities that lets
	us behave as a Database
	"""
	def __init__(self) -> None:
		super().__init__()
		self.db = Database()
		self.set = self.db.set
	

	def _start(self):
		super()._start()
		self.query_reply_queue = Queue()
		init = Thread(
			target= self.init_db,
			daemon= True,
		)
		init.start()
	

	def send_set(self, index: int, word: str):
		"""
		Update db and broadcast SET command to all known peers
		"""
		self.set(index, word)
		with self.sock.lock:
			for peer in self.get_peers():
				peer.set(self.sock.obj, index, word)
	

	def init_db(self):
		"""
		Try to initialize the db in a separate thread by randomly
		querying peers until one returns a valid db
		"""
		data = json.dumps({"command": "QUERY"}).encode()
		count = 0
		bad_peers = set()
		while True:
			try:
				peer = random.choice(self.get_peers())
				if peer in bad_peers:
					count += 1
					if len(bad_peers) == len(self.get_peers()) or count >= 10:
						logging.debug("Abandoning DB initialization.")
						return
					continue

			except IndexError:
				count += 1
				if count >= 10:
					logging.debug("Abandoning DB initialization.")
					return
				sleep(1)
			
			else:
				while self.query_reply_queue.qsize() != 0:
					self.query_reply_queue.get()

				with self.sock.lock:
					peer.send(self.sock.obj, data)

				try:
					db = self.query_reply_queue.get(timeout= 5)
				except Empty:
					logging.debug(f"{peer} didn't reply to QUERY command")
					bad_peers.add(peer)
				else:
					try:
						assert isinstance(db, list) and len(db) == DB_SIZE
						for word in db:
							assert bool(isinstance(word, str) or word is None)
					except AssertionError:
						logging.debug(f"{peer} sent bad database: {db}")
						bad_peers.add(peer)
					else:
						break

		if not any(self.db.all()):
			logging.debug(f"Initialize DB to {db} gotten from {peer}")
			self.db.reset(db)
	

	def query_received(self, addr):
		"""
		A query request has been received
		"""
		reply = json.dumps(
			{
				"command": "QUERY-REPLY",
				"database": str(self.db.all()),
			}
		).encode()
		logging.debug(f"Replying query with {reply}")
		with self.sock.lock:
			self.sock.obj.sendto(reply, addr)
	

	def query_reply_received(self, db: list[str | None]):
		"""
		Reply to a query has been received.
		We add the response to a queue which never gets read
		unless we requested a query in the first place
		"""
		self.query_reply_queue.put(db)
	

	def cli_current(self, sock: socket):
		"""
		Client has requested current word list over sock
		"""
		sock.sendall(str(self.db.all()).encode() + b"\n")
	

	def cli_lie(self, sock: socket):
		"""
		Start lying
		"""
		self.db.lie()
		sock.sendall(b"I'd never lie to you, of course :)\n")
	

	def cli_truth(self, sock: socket):
		"""
		Stop lying
		"""
		self.db.truth()
		sock.sendall(b"You got it, boss.\n")
	

	def cli_set(self, sock: socket, index, word):
		"""
		Send SET command to all known peers
		"""
		sock.sendall(f"Setting {index} to {word}...\n".encode())
		self.send_set(int(index), str(word))
		sock.sendall(b"Done!\n")