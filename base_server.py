from abc import ABC, abstractmethod


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