import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Union, FrozenSet, Callable, Optional, Tuple, List

from Bio import SeqIO

from bioseq_dataset.lmdb import LMDBWrapper


@dataclass
class SequenceData:
    seq: str
    seq_classes: FrozenSet[str]
    label: int

    def __str__(self):
        return f"{self.seq},{';'.join(self.seq_classes)},{self.label}"

    @staticmethod
    def from_string(string: str):
        seq, seq_classes, label = string.split(",")
        seq_classes = frozenset(seq_classes.split(";"))
        return SequenceData(seq=seq, seq_classes=seq_classes, label=int(label))

    def __eq__(self, other):
        if not isinstance(other, SequenceData):
            if isinstance(other, str):
                return self.seq == other
            else:
                return False
        return self.seq == other.seq

    def __hash__(self):
        return hash(self.seq)


class SequenceDataset:
    def __init__(
        self,
        db_path: Union[str, os.PathLike],
        read_only: bool = True,
        map_size: int = 31457280,
    ):
        self.db = LMDBWrapper(db_path, read_only, map_size)
        self.hash_table = None

    def init_db(self) -> None:
        self.db.init_db()
        self._build_hash()

    def _build_hash(self):
        self.hash_table = defaultdict(list)
        keys = self.get_keys()
        for key in keys:
            self.hash_table[hash(self.get_sequence(key))].append(key)

    def add_sequence(
        self, key: str, seq: SequenceData, merge_duplicates: bool = True
    ) -> None:
        if merge_duplicates:
            duplicate = self.get_duplicate(seq)  # duplicate is a pair (key, seq_data)
            if duplicate is not None:
                if seq.label != duplicate[1].label:
                    raise ValueError("Conflicting labels for identical sequences")
                seq = SequenceData(
                    seq.seq, seq.seq_classes | duplicate[1].seq_classes, seq.label
                )
                self.db.remove(duplicate[0])
        self.db.write(key, str(seq))
        self.hash_table[hash(seq)].append(key)

    def get_sequence(self, key: str) -> SequenceData:
        seq_data = self.db.read(key)
        if seq_data is None:
            return seq_data
        return SequenceData.from_string(seq_data)

    def remove_sequence(self, key: str) -> None:
        seq_data = self[key]
        if seq_data is not None:
            self.db.remove(key)
            if key in self.hash_table[hash(seq_data)]:
                self.hash_table[hash(seq_data)].remove(key)

    def get_keys(self) -> List[str]:
        return self.db.get_keys()

    def get_duplicate(
        self, item: Union[str, SequenceData]
    ) -> Optional[Tuple[str, SequenceData]]:
        for key in self.hash_table[hash(item)]:
            if self[key] == item:
                return key, self[key]

    def parse_fasta(
        self,
        filepath: Union[str, os.PathLike],
        class_map: Callable[[str], FrozenSet[str]],
        label_map: Callable[[str], int],
        merge_duplicates: bool = True,
    ) -> None:
        for entry in SeqIO.parse(filepath, "fasta"):
            seq = SequenceData(str(entry.seq), class_map(entry.id), label_map(entry.id))
            self.add_sequence(entry.id, seq, merge_duplicates)

    def commit(self) -> None:
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def __getitem__(self, item: str) -> SequenceData:
        return self.get_sequence(item)

    def __contains__(self, item: Union[str, SequenceData]):
        for key in self.hash_table[hash(item)]:
            if self[key] == item:
                return True
        return False

    def __len__(self):
        return len(self.db)
