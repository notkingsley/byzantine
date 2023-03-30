from abc import ABC, abstractmethod
import logging
import os
import socket
import sys

from locked import Locked


class AbstractBaseServer(ABC):
	"""
	Defines common methods servers ought have
	"""
	def __init__(self) -> None:
		super().__init__()
	

	@abstractmethod
	def _start(self):
		pass


	@abstractmethod
	def _stop(self):
		pass


class BaseServer(AbstractBaseServer):
	"""
	The BaseServer allows us to send and receive message
	over a UDP socket
	"""
	def __init__(self) -> None:
		super().__init__()
		self.sock = Locked(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))

		port = 0
		if len(sys.argv) > 1:
			port = int(sys.argv[1])

		# no need to lock here
		self.sock.obj.bind(("", port))
		logging.debug(f"Bound to {self.sock.obj.getsockname()}")

		self.host = self.sock.obj.getsockname()[0]
		self.port = self.sock.obj.getsockname()[1]
		self.name = f"peer-{os.getpid()}"
	

	def _stop(self):
		"""
		Complement of _start. Probably stop what _start started
		Here, we close the socket
		"""
		super()._stop()
		self.sock.obj.close()