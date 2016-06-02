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
	for (i=0; i<2000000; i++)
	{
		val = rand();
		rc = mdb_put(txn, dbi, &mkey, &mval, 0);
		assert(rc == 0);
	}

	rc = mdb_txn_commit(txn);
	assert(rc == 0);

	printf("initial state load.\n");

	// try out navigating the dup value space
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
	printf("count: %zu\n", count);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_GET_MULTIPLE);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT_MULTIPLE);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT_MULTIPLE);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT_MULTIPLE);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT_MULTIPLE);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_LAST_DUP);
	assert(rc == 0);
	printf("%lu\n", mval.mv_size / sizeof(val));
	printf("key: %d val: %d\n",
		*(unsigned int *) mkey.mv_data, *(unsigned int *) mval.mv_data);

	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	// close
	mdb_env_close(env);
	return 0;
}
