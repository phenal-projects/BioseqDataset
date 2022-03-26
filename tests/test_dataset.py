import os

from bioseq_dataset.sequence_dataset.dataset import SequenceData, SequenceDataset


def test_sequence_data():
    data = SequenceData("abc", frozenset(["a", "b", "c"]), 1)
    string = str(data)

    assert data == SequenceData.from_string(
        string
    ), "Data is corrupted during serialization/deserialization"


def test_sequence_dataset():
    # prepare folders
    if os.path.exists("./tmp/db.lmdb"):
        os.remove("./tmp/db.lmdb")
    os.makedirs("./tmp", exist_ok=True)

    # prepare fasta file
    with open("./tmp/test.fasta", "w") as fout:
        fout.write(">test_id description\n")
        fout.write("AAAGGG\n")

    # test destructor without init
    dataset = SequenceDataset("./tmp/db.lmdb", read_only=False)
    del dataset

    # test parsing
    dataset = SequenceDataset("./tmp/db.lmdb", read_only=False)
    dataset.init_db()

    dataset.parse_fasta(
        "./tmp/test.fasta",
        class_map=lambda x: frozenset(("2", "3")),
        label_map=lambda x: 0,
    )
    assert SequenceData("AAAGGG", frozenset(("2", "3")), 0) == dataset.get_sequence(
        "test_id"
    ), "Parsing is incorrect"
    del dataset

    # clean up
    os.remove("./tmp/db.lmdb")
    os.remove("./tmp/test.fasta")
    os.removedirs("./tmp")
