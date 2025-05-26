from typing import Generic, TypeVar, Optional, List

from pydantic import BaseModel


T = TypeVar('T')
class CustomSet(BaseModel, Generic[T]):
    _set: set[T] = set()

    def put(self, item: T) -> None:
        self._set.add(item)

    def get(self, item: T) -> Optional[T]:
        return item if item in self._set else None

    def list(self) -> List[T]:
        return list(self._set)

    def remove(self, item: T) -> None:
        self._set.discard(item)

    def contains(self, item: T) -> bool:
        return item in self._set

    def clear(self) -> None:
        self._set.clear()

    def size(self) -> int:
        return len(self._set)
