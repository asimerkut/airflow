from typing import Dict, Generic, TypeVar, Optional, Iterable, Tuple

from pydantic import BaseModel


K = TypeVar("K")
V = TypeVar("V")
class CustomMap(BaseModel, Generic[K, V]):
    _map: Dict[K, V] = {}

    def clear(self) -> None:
        self._map.clear()

    def has(self, key: K) -> bool:
        return key in self._map

    def get(self, key: K) -> Optional[V]:
        return self._map.get(key)

    def add(self, key: K, val: V) -> None:
        self._map[key] = val

    def remove(self, key: K) -> None:
        self._map.pop(key, None)

    def get_keys(self) -> Iterable[K]:
        return self._map.keys()

    def get_values(self) -> Iterable[V]:
        return self._map.values()

    def get_items(self) -> Iterable[Tuple[K, V]]:
        return self._map.items()

    def get_connector(self, flow_id, alias) -> Optional[V]:
        return self.get(alias)

    def size(self) -> int:
        return len(self._map)
