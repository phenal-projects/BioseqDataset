import os
from dataclasses import dataclass
from typing import Union, FrozenSet, Callable

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


class SequenceDataset:
    def __init__(self, db_path: Union[str, os.PathLike], read_only: bool = True):
        self.db = LMDBWrapper(db_path, read_only)

    def init_db(self) -> None:
        self.db.init_db()

    def add_sequence(self, name: str, seq: SequenceData) -> None:
        self.db.write(name, str(seq))

    def get_sequence(self, name: str) -> SequenceData:
        return SequenceData.from_string(self.db.read(name))

    def get_keys(self):
        return self.db.get_keys()

    def parse_fasta(
            self,
            filepath: Union[str, os.PathLike],
            class_map: Callable[[str], FrozenSet[str]],
            label_map: Callable[[str], int],
    ) -> None:
        for entry in SeqIO.parse(filepath, "fasta"):
            seq = SequenceData(str(entry.seq), class_map(entry.id), label_map(entry.id))
            self.add_sequence(entry.id, seq)

    def commit(self) -> None:
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def __len__(self):
        return len(self.db)

    def __del__(self):
        self.commit()
        self.close()
