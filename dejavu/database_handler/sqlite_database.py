from __future__ import absolute_import
from itertools import zip_longest
import queue as Queue
import pprint

import sqlite3

from dejavu.base_classes.common_database import CommonDatabase
from dejavu.config.settings import (FIELD_FILE_SHA1, FIELD_FINGERPRINTED,
                                    FIELD_HASH, FIELD_OFFSET, FIELD_SONG_ID,
                                    FIELD_SONGNAME, FIELD_TOTAL_HASHES,
                                    FINGERPRINTS_TABLENAME, SONGS_TABLENAME)

class SQLiteDatabase(CommonDatabase):
    type = "sqlite3"


    # creates
    CREATE_SONGS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
                %s INTEGER PRIMARY KEY AUTOINCREMENT,
                %s STRING NOT NULL,
                %s INTEGER DEFAULT 0,
                %s TEXT NOT NULL,
                %s INTEGER DEFAULT 0
        )
    """ % (
        SONGS_TABLENAME,
        FIELD_SONG_ID,
        FIELD_SONGNAME,
        FIELD_FINGERPRINTED,
        FIELD_FILE_SHA1,
        FIELD_TOTAL_HASHES,
    )

    CREATE_FINGERPRINTS_TABLE = """
        CREATE TABLE IF NOT EXISTS `%s` (
             `%s` binary not null,
             `%s` INTEGER not null,
             `%s` INTEGER not null,
         UNIQUE(%s, %s, %s),
         FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
    ) """ % (
        FINGERPRINTS_TABLENAME, FIELD_HASH,
        FIELD_SONG_ID, FIELD_OFFSET,
        FIELD_SONG_ID, FIELD_OFFSET, FIELD_HASH,
        FIELD_SONG_ID, SONGS_TABLENAME, FIELD_SONG_ID
    )

    # inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        INSERT OR IGNORE INTO %s (%s, %s, %s) values
            (?, ?, ?);
    """ % (FINGERPRINTS_TABLENAME, FIELD_HASH, FIELD_SONG_ID, FIELD_OFFSET)

    INSERT_SONG = "INSERT INTO %s (%s, %s, %s) values (?, ?, ?)" % (
        SONGS_TABLENAME, FIELD_SONGNAME, FIELD_FILE_SHA1, FIELD_TOTAL_HASHES)

    # selects
    SELECT = """
        SELECT %s, %s FROM %s WHERE %s = ?
    """ % (FIELD_SONG_ID, FIELD_OFFSET, FINGERPRINTS_TABLENAME, FIELD_HASH)

    SELECT_MULTIPLE = """
        SELECT %s, %s, %s, %s FROM %s WHERE %s IN (%%s)
    """ % (FIELD_SONG_ID, FIELD_HASH, FIELD_SONG_ID, FIELD_OFFSET,
           FINGERPRINTS_TABLENAME, FIELD_HASH)

    SELECT_ALL = """
        SELECT %s, %s FROM %s;
    """ % (FIELD_SONG_ID, FIELD_OFFSET, FINGERPRINTS_TABLENAME)

    SELECT_SONG = """
        SELECT %s, %s as %s, %s FROM %s WHERE %s = ?
    """ % (FIELD_SONGNAME, FIELD_FILE_SHA1, FIELD_FILE_SHA1, FIELD_TOTAL_HASHES, SONGS_TABLENAME, FIELD_SONG_ID)

    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n FROM %s
    """ % (FINGERPRINTS_TABLENAME)

    SELECT_UNIQUE_SONG_IDS = """
        SELECT COUNT(DISTINCT %s) AS n FROM %s WHERE %s = 1;
    """ % (FIELD_SONG_ID, SONGS_TABLENAME, FIELD_FINGERPRINTED)

    SELECT_SONGS = """
        SELECT %s AS %s, %s, %s as %s FROM %s WHERE %s = 1;
    """ % (FIELD_SONG_ID, FIELD_SONG_ID, FIELD_SONGNAME, FIELD_FILE_SHA1, FIELD_FILE_SHA1,
           SONGS_TABLENAME, FIELD_FINGERPRINTED)

    # drops
    DROP_FINGERPRINTS = "DROP TABLE IF EXISTS %s;" % FINGERPRINTS_TABLENAME
    DROP_SONGS = "DROP TABLE IF EXISTS %s;" % SONGS_TABLENAME

    # update
    UPDATE_SONG_FINGERPRINTED = """
        UPDATE %s SET %s = 1 WHERE %s = ?
    """ % (SONGS_TABLENAME, FIELD_FINGERPRINTED, FIELD_SONG_ID)

    # delete
    DELETE_UNFINGERPRINTED = """
        DELETE FROM %s WHERE %s = 0;
    """ % (SONGS_TABLENAME, FIELD_FINGERPRINTED)


    IN_MATCH = f"?"

    def __init__(self, **options):
        super(SQLiteDatabase, self).__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self):
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.

        This also removes all songs that have been added but have no
        fingerprints associated with them.
        """
        with self.cursor() as cur:
            # print(self.CREATE_SONGS_TABLE)
            # print(self.CREATE_FINGERPRINTS_TABLE)
            # print(self.DELETE_UNFINGERPRINTED)
            cur.execute(self.CREATE_SONGS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            # cur.execute(self.CREATE_LOGGING_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def empty(self):
        """
        Drops tables created by dejavu and then creates them again
        by calling `SQLDatabase.setup`.

        .. warning:
            This will result in a loss of data
        """
        with self.cursor() as cur:
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_SONGS)

        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Removes all songs that have no fingerprints associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_songs(self):
        """
        Returns number of songs the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_UNIQUE_SONG_IDS)

            for count, in cur:
                return count
            return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)

            for count, in cur:
                return count
            return 0

    def set_song_fingerprinted(self, sid):
        """
        Set the fingerprinted flag to TRUE (1) once a song has been completely
        fingerprinted in the database.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_SONG_FINGERPRINTED, (sid,))

    def get_songs(self):
        """
        Return songs that have the fingerprinted flag set TRUE (1).
        """
        with self.cursor() as cur:
            #print(self.SELECT_SONGS)
            cur.execute(self.SELECT_SONGS)
            for row in cur:
                yield row

    def get_song_by_id(self, sid):
        """
        Returns song by its ID.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_SONG, (sid,))
            return cur.fetchone()

    def insert(self, hash, sid, offset):
        """
        Insert a (sha1, song_id, offset) row into database.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, (hash.encode(), sid, offset))

    def insert_log(self, content):
        with self.cursor() as cur:
            cur.execute(self.INSERT_LOG, (content,))

    def insert_song(self, songname, file_hash, total_hashes):
        """
        Inserts song in the database and returns the ID of the inserted record.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_SONG, (songname, file_hash, total_hashes))
            return cur.lastrowid

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        with self.cursor() as cur:
            if hash is None:
                cur.execute(self.SELECT_ALL)
            else:
                cur.execute(self.SELECT, [hash.encode()])

            for result in cur.fetchall():
                yield (result[FIELD_SONG_ID], result[FIELD_OFFSET])

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        values = []
        for ahash, offset in hashes:
            values.append((ahash.encode(), sid, int(offset)))

        with self.cursor() as cur:
            for split_values in grouper(values, 100):
                cur.executemany(self.INSERT_FINGERPRINT, list(split_values))

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        mapper = {}        
        # for hash, offset in hashes:
        #     mapper[hash.encode()] = offset

        for hsh, offset in hashes:
            if hsh.upper() in mapper.keys():
                mapper[hsh.encode().upper()].append(offset)
            else:
                mapper[hsh.encode().upper()] = [offset]

        # Get an iteratable of all the hashes we need
        values = mapper.keys()

        dedup_hashes = {}

        results = []
        with self.cursor() as cur:
            for split_values in grouper(values, 998):
                split_list = list(split_values)
                # Create our IN part of the query
                query = self.SELECT_MULTIPLE
                query = query % ', '.join(['?'] * len(split_list))
                # print(query)

                for result in cur.execute(query, split_list).fetchall():
                    # (sid, db_offset - song_sampled_offset)
                    try:
                        returned_offset = result[FIELD_OFFSET]
                        returned_hash = result[FIELD_HASH]
                        #original_offset = mapper[returned_hash]

                        #adjusted_offset = returned_offset - original_offset

                        returned_song_id = result[FIELD_SONG_ID]

                        #yield (returned_song_id, adjusted_offset)

                        if returned_song_id not in dedup_hashes.keys():
                            dedup_hashes[returned_song_id] = 1
                        else:
                            dedup_hashes[returned_song_id] += 1

                        for song_sampled_offset in mapper[returned_hash]:
                            results.append((returned_song_id, returned_offset - song_sampled_offset))


                        # results.append((returned_song_id, adjusted_offset))

                        # results.append(returned_song_id, adjusted_offset) 
                    except TypeError as e:
                        pprint.pprint(e)
                        raise Exception("Result: {}\nOffset From Map: {}\nRehashed Hash: {}".format(result, original_offset, hash.encode()))
            # pprint.pprint(results)
            # pprint.pprint(dedup_hashes)
            return results, dedup_hashes


    def __getstate__(self):
        return (self._options,)

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in zip_longest(fillvalue=fillvalue, *args))

def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)
    return cursor

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]

    return d
    
def setup_db(db_path, dict_factory = dict_factory):
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.


    ```python
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
    ```
    """
    _cache = Queue.Queue(maxsize=5)

    def __init__(self, cursor_type=sqlite3.Cursor, **options):
        super(Cursor, self).__init__()

        try:
            conn = self._cache.get_nowait()
        except Queue.Empty:
            # conn = sqlite3.connect(**options)
            conn = setup_db(**options)
        else:
            nothing = None

        self.conn = conn

    @classmethod
    def clear_cache(cls):
        cls._cache = Queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a SQL related error we try to rollback the cursor.
        if extype is sqlite3.Error:
            self.cursor.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except Queue.Full:
            self.conn.close()