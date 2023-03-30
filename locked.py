from threading import Lock
from typing import Generic, TypeVar


Tp = TypeVar("Tp")


class Locked(Generic[Tp]):
	"""
	A convenience for binding a shared Tp object to a Lock()
	Never access obj without acquiring lock
	"""
	def __init__(self, obj: Tp) -> None:
		self.obj = obj
		self.lock = Lock()