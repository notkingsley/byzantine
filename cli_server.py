import logging
import socket

from base_server import AbstractBaseServer

CLI_PORT = 0


class CLIServer(AbstractBaseServer):
	def __init__(self) -> None:
		super().__init__()
		self.cli_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.cli_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.cli_server.setblocking(False)


	def _start(self):
		"""
		Bind and listen to CLI port
		"""
		super()._start()
		self.cli_server.bind(("", CLI_PORT))
		self.cli_server.listen()
		logging.debug(f"CLI client at {self.cli_server.getsockname()}")
	
	
	def _stop(self):
		"""
		Close the CLI socket
		"""
		super()._stop()
		logging.debug("Closing CLI socket")
		self.cli_server.close()


	def format_peers(self) -> str: ...
	

	def dispatch(self, sock: socket.socket, command: str = "", *args):
		"""
		command was received from the cli over the sock connection 
		and args are the arguments, dispatch to the necessary function
		"""
		func = getattr(self, "cli_" + command, None)
		if not func:
			sock.sendall(f"I don't seem to implement that command: {command}\n".encode())
		
		else:
			func(sock, *args)
	

	def cli_(self, *args):
		"""
		No command; no-op
		"""
	

	def cli_peers(self, sock: socket.socket):
		"""
		peers command received from the client
		Lists all known peers
		"""
		sock.sendall(self.format_peers().encode() + b"\n")
	

	def cli_exit(self, sock: socket.socket):
		"""
		Close this client
		"""
		sock.sendall("Later, loser!".encode() + b"\n")
		logging.debug(f"Client {sock.getsockname()} disconnected.")
		self.selector.unregister(sock)
		sock.close()