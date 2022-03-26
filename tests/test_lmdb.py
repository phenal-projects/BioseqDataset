import os.path
import random
import shutil
import string

from bioseq_dataset.lmdb import LMDBWrapper


def test_lmdb_wrapper():
    # prepare folders
    shutil.rmtree("./tmp/", ignore_errors=True)
    os.makedirs("./tmp")

    lmdb = LMDBWrapper("./tmp/db.lmdb", read_only=False)
    lmdb.init_db()

    lmdb.write("a", "abc")
    value = lmdb.read("a")
    assert value == "abc", "Value is corrupted"

    # test len
    lmdb.write("b", "abc")
    lmdb.write("c", "abc")
    lmdb.write("d", "abc")
    assert len(lmdb) == 4, "Incorrect __len__"

    # test get_keys
    assert len(set(lmdb.get_keys()) ^ {"a", "b", "c", "d"})

    # test loading
    del lmdb
    lmdb = LMDBWrapper("./tmp/db.lmdb", read_only=True)
    lmdb.init_db()

    value = lmdb.read("a")
    assert value == "abc", "Value is corrupted during loading"

    # clean up
    shutil.rmtree("./tmp/", ignore_errors=True)


def test_lmdb_wrapper_under_load():
    # prepare folders
    shutil.rmtree("./tmp/", ignore_errors=True)
    os.makedirs("./tmp")

    lmdb = LMDBWrapper("./tmp/db.lmdb", read_only=False)
    lmdb.init_db()

    # write 200*1024 random sequences
    sequences = []
    for i in range(200 * 1024):
        random_sequence = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=256)
        )
        sequences.append(random_sequence)
        lmdb.write(str(i), random_sequence)
        if ((i + 1) % 1000) == 0:
            lmdb.commit()
    lmdb.commit()

    for i in range(200 * 1024):
        assert sequences[i] == lmdb.read(str(i)), "Some sequences are corrupted"

    # clean up
    shutil.rmtree("./tmp/", ignore_errors=True)
