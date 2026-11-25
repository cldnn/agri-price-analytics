"""Migrate a six-source raw collection to five sources. Run with python -m ingestion.migrate_source_ids."""
import sqlite3
from contextlib import ExitStack, closing

from collect_raw import META, RAW, writer_lock
from ingestion.sources import CODE_OPS, require


CHANGES = (
    ("DS-002", None, ("trades",)),
    ("DS-003", "DS-002", ("getWthrDataList",)),
    ("DS-004", "DS-003", ("getItemtradeList",)),
    ("DS-005", "DS-004", ("dateAvgRecentPrice.do",)),
    ("DS-006", "DS-005", CODE_OPS),
)
ARCHIVE = RAW / "_archive" / "source-ids-v1"


def move(source, destination):
    root = RAW.resolve()
    require(all(p.resolve().is_relative_to(root) and p.resolve() != root for p in (source, destination)),
            "unexpected migration path")
    require(not destination.exists(), f"destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source.rename(destination)


def migrate():
    state = META / "state.sqlite3"
    require(state.is_file(), "no collection database to migrate")
    with closing(sqlite3.connect(state, timeout=60)) as db, ExitStack() as locks:
        if db.execute("PRAGMA user_version").fetchone()[0] == 1:
            print("Source numbers are already current.")
            return
        require(db.execute("PRAGMA user_version").fetchone()[0] == 0, "unknown database version")
        runs = [row[0] for row in db.execute("SELECT run FROM runs")]
        for run in runs:
            for source in ["DS-001", *(old for old, _, _ in CHANGES)]:
                locks.enter_context(writer_lock(run, source))
        db.execute("BEGIN IMMEDIATE")
        archive_state = ARCHIVE / "_meta" / "state.sqlite3"
        archive_state.parent.mkdir(parents=True, exist_ok=True)
        if not archive_state.exists():
            temp = archive_state.with_suffix(".partial")
            with closing(sqlite3.connect(state)) as reader, closing(sqlite3.connect(temp)) as copy:
                reader.backup(copy)
                require(copy.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "invalid archive backup")
            move(temp, archive_state)

        # Filesystem renames can survive an interrupted SQL transaction. Identify each
        # source by its operation directory, so the same command resumes safely.
        for old, new, ops in CHANGES:
            actual = {r[0] for r in db.execute("SELECT DISTINCT op FROM units WHERE source=?", (old,))}
            require(actual <= set(ops), f"unexpected legacy operations for {old}")
            before = RAW / f"source={old}"
            after = RAW / f"source={new}" if new else ARCHIVE / before.name
            if any((before / f"operation={op}").is_dir() for op in ops):
                move(before, after)
            saved_ops = db.execute("""SELECT DISTINCT u.op FROM units u JOIN pages p ON p.unit=u.id
                                      WHERE u.source=?""", (old,))
            for op, in saved_ops:
                require((after / f"operation={op}").is_dir(), f"missing stored {old} {op} files")
            for ext in ("log", "err"):
                for path in META.glob(f"{old}*.{ext}"):
                    move(path, archive_state.parent / path.name)
        if (META / "DS-004").exists():
            move(META / "DS-004", META / "DS-003")

        # The complete pre-migration manifest and the auction files remain together in the archive.
        for old, new, _ in CHANGES:
            if new is None:
                db.execute("DELETE FROM pages WHERE unit IN (SELECT id FROM units WHERE source=?)", (old,))
                db.execute("DELETE FROM units WHERE source=?", (old,))
            else:
                prefix = f"source={old}/"
                bad = db.execute("""SELECT 1 FROM pages p JOIN units u ON u.id=p.unit
                                    WHERE u.source=? AND substr(path,1,?)<>? LIMIT 1""",
                                 (old, len(prefix), prefix)).fetchone()
                require(not bad, f"unexpected path for {old}")
                db.execute("""UPDATE pages SET path=? || substr(path,?)
                              WHERE unit IN (SELECT id FROM units WHERE source=?)""",
                           (f"source={new}/", len(prefix) + 1, old))
                db.execute("UPDATE units SET source=? WHERE source=?", (new, old))
        db.execute("PRAGMA user_version=1")
        db.commit()
    for run in runs:
        obsolete_lock = META / f"{run}-DS-006.lock"
        if obsolete_lock.exists():
            move(obsolete_lock, archive_state.parent / obsolete_lock.name)
    print(f"Migrated active sources. Historical manifest and auction files: {ARCHIVE}")


if __name__ == "__main__":
    migrate()
