import os.path

from bioseq_dataset.lmdb import LMDBWrapper


def test_lmdb_wrapper():
    # prepare folders
    if os.path.exists("./tmp/db.lmdb"):
        os.remove("./tmp/db.lmdb")
        os.removedirs("./tmp")
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
    os.remove("./tmp/db.lmdb")
    os.removedirs("./tmp")
