import os
import shutil

from bioseq_dataset.sequence_dataset.dataset import SequenceData, SequenceDataset


def test_sequence_data():
    data = SequenceData("abc", frozenset(["a", "b", "c"]), 1)
    string = str(data)

    assert data == SequenceData.from_string(
        string
    ), "Data is corrupted during serialization/deserialization"


def test_sequence_dataset():
    # prepare folders
    shutil.rmtree("./tmp/", ignore_errors=True)
    os.makedirs("./tmp")

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
    assert "AAAGGG" == dataset["test_id"], "Retrieved sequence is incorrect"
    assert (
        frozenset(("2", "3")) == dataset["test_id"].seq_classes
    ), "Retrieved classes are incorrect"
    assert 0 == dataset["test_id"].label, "Retrieved label is incorrect"

    del dataset

    # clean up
    shutil.rmtree("./tmp/", ignore_errors=True)


def test_sequence_dataset_duplicates_test():
    # prepare folders
    shutil.rmtree("./tmp/", ignore_errors=True)
    os.makedirs("./tmp")

    # test duplicates
    dataset = SequenceDataset("./tmp/db.lmdb", read_only=False)
    dataset.init_db()

    dataset.add_sequence(
        "1", SequenceData("ab", frozenset("ab"), 0), merge_duplicates=True
    )
    dataset.add_sequence(
        "2", SequenceData("ab", frozenset("c"), 0), merge_duplicates=True
    )
    assert "ab" == dataset["2"], "Merging was not successful: sequence is corrupted"
    assert (
        frozenset("abc") == dataset["2"].seq_classes
    ), "Merging was not successful: classes are corrupted"
    assert 0 == dataset["2"].label, "Merging was not successful: label is corrupted"
    assert dataset["1"] is None, "Merging was not successful: old entry is not removed"

    try:
        dataset.add_sequence(
            "3", SequenceData("ab", frozenset("c"), 1), merge_duplicates=True
        )
        assert False, "No exception from merging conflicts"
    except:
        pass

    # clean up
    shutil.rmtree("./tmp/", ignore_errors=True)
