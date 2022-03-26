import os
from io import UnsupportedOperation
from typing import Union, List, Optional

import lmdb


class LMDBWrapper:
    def __init__(
        self,
        db_path: Union[str, os.PathLike] = "./data/sequence.lmdb",
        read_only: bool = True,
        map_size: int = 32212254720,
    ):
        self.db_path = db_path
        self.read_only = read_only
        self.map_size = map_size

        self.env = None
        self.txn = None

    def init_db(self) -> None:
        self.env = lmdb.open(
            self.db_path,
            map_size=self.map_size,
            subdir=os.path.isdir(self.db_path),
            readonly=self.read_only,
            lock=not self.read_only,
            readahead=False,
            meminit=False,
        )
        self.txn = self.env.begin(write=not self.read_only)

    def read(self, key: str) -> Optional[str]:
        if self.txn is None:
            raise AttributeError("The database is not initialized")
        lmdb_data = self.txn.get(key.encode("ascii"))

        if lmdb_data is None:
            return None
        return lmdb_data.decode("ascii")

    def write(self, key: str, data: str) -> None:
        if self.read_only:
            raise UnsupportedOperation("Not writable")
        if self.txn is None:
            raise AttributeError("The database is not initialized")

        self.txn.put(key.encode("ascii"), data.encode("ascii"))

    def remove(self, key: str) -> None:
        if self.txn.delete(
            key.encode("ascii"),
        ):
            self.commit()  # commit changes

    def __len__(self) -> int:
        if self.txn is None:
            raise AttributeError("The database is not initialized")
        return self.txn.stat()["entries"]

    def get_keys(self) -> List[str]:
        if self.txn is None:
            raise AttributeError("The database is not initialized")
        return list(self.txn.cursor().iternext(values=False))

    def commit(self) -> None:
        if self.txn is not None:
            self.txn.commit()
            self.txn = self.env.begin(write=not self.read_only)

    def close(self) -> None:
        if self.env is not None:
            self.env.close()

    def __del__(self):
        self.commit()
        self.close()
