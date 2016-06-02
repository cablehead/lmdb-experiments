#include <stdlib.h>
#include <sys/errno.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>

#include "lmdb.h"


const unsigned int COMPACT =
	MDB_DUPSORT | MDB_INTEGERKEY | MDB_DUPFIXED | MDB_INTEGERDUP;


int main(int argc,char * argv[])
{
	int rc;
	int i;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_txn *txn;
	MDB_cursor *cursor;

	MDB_val mkey, mval;
	unsigned int key;
	unsigned int val;

	mkey.mv_size = sizeof(unsigned int);
	mkey.mv_data = &key;
	mval.mv_size = sizeof(unsigned int);
	mval.mv_data = &val;

	srand(time(NULL));

	// open
	rc = mdb_env_create(&env);
	assert(rc == 0);
	rc = mdb_env_set_mapsize(env, 100*1024*1024);
	assert(rc == 0);
	rc = mdb_env_open(env, "./testdb", 0, 0664);
	assert(rc == 0);

	// load initial state
	rc = mdb_txn_begin(env, NULL, 0, &txn);
	assert(rc == 0);
	rc = mdb_dbi_open(txn, NULL, COMPACT, &dbi);
	assert(rc == 0);
	rc = mdb_txn_commit(txn);
	assert(rc == 0);

	key = rand();
	printf("key: %d\n", key);

	rc = mdb_txn_begin(env, NULL, 0, &txn);
	assert(rc == 0);
	int to_del;
	for (i=0; i<2000000; i++)
	{
		val = rand();
		if (i==1000000) to_del = val;
		rc = mdb_put(txn, dbi, &mkey, &mval, 0);
		assert(rc == 0);
	}

	rc = mdb_txn_commit(txn);
	assert(rc == 0);

	printf("initial state load.\n\n");

	// full scan
	rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	assert(rc == 0);
	rc = mdb_cursor_open(txn, dbi, &cursor);
	assert(rc == 0);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET);
	assert(rc == 0);
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	size_t count;
	rc = mdb_cursor_count(cursor, &count);
	assert(rc == 0);
	printf("count: %zu\n\n", count);

	int n;
	int seen = 0;
	int alt = 0;
	while ((rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT_MULTIPLE)) == 0)
	{
		n = mval.mv_size / sizeof(val);
		seen += n;
		if (alt % 100 == 0)
		{
			printf("key: %d val: %d n: %d\n",
				*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data, n);
		}
		alt++;
	}
	printf("traversed: %d\n\n", seen);
	assert(rc == MDB_NOTFOUND);

	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	// delete an item
	rc = mdb_txn_begin(env, NULL, 0, &txn);
	assert(rc == 0);
	val = to_del;
	printf("delete: %d\n", val);
	rc = mdb_del(txn, dbi, &mkey, &mval);
	assert(rc == 0);
	rc = mdb_txn_commit(txn);
	assert(rc == 0);

	rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	assert(rc == 0);
	rc = mdb_cursor_open(txn, dbi, &cursor);
	assert(rc == 0);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET);
	assert(rc == 0);
	rc = mdb_cursor_count(cursor, &count);
	assert(rc == 0);

	printf("count: %zu\n\n", count);
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	// close
	mdb_env_close(env);
	return 0;
}
