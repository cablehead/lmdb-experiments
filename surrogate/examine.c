#include <stdio.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include "lmdb/libraries/liblmdb/lmdb.h"
#include "blake2/sse/blake2.h"
#include "blake2/sse/blake2-impl.h"

const int NUM_BYTES = 17;
const size_t MAX_KEY_COUNT= 1000;
const unsigned int FLAGS = MDB_DUPSORT |  MDB_DUPFIXED | MDB_CREATE;


int
main(int argc, char * argv[]) {

	// set up variables
	int rc, j;
	size_t count;
	char * max_keys;
	MDB_env *env;
        MDB_dbi dbi, dbi_rev;
        MDB_txn *txn;
        MDB_cursor *cursor, *cursor_rev;
	
	// set up keys and values
	MDB_val mkey, mval;
        char key [NUM_BYTES];
        char val [NUM_BYTES];

        mkey.mv_size = NUM_BYTES;
        mkey.mv_data = &key;
        mval.mv_size = NUM_BYTES;
        mval.mv_data = &val;

	// initialize environment; set 2 database limit
        rc = mdb_env_create (&env);
        assert (rc == 0);
	rc = mdb_env_set_maxdbs (env, 2);
        assert (rc == 0);
        rc = mdb_env_open (env, "./db_dir", 0, 0664);
        assert (rc == 0);

	// begin transaction
        rc = mdb_txn_begin (env, NULL, 0, &txn);
        assert (rc == 0);

        // open databases
        rc = mdb_dbi_open (txn, "data_store", FLAGS, &dbi);
        assert (rc == 0);
        rc = mdb_dbi_open (txn, "rev_data_store", FLAGS, &dbi_rev);
        assert(rc == 0);

        // initiate cursors
        rc = mdb_cursor_open (txn, dbi, &cursor);
        assert (rc == 0);
	rc = mdb_cursor_open (txn, dbi_rev, &cursor_rev);
	assert (rc == 0);

	// position cursor at beginning of database
	rc = mdb_cursor_get (cursor, &mkey, &mval, MDB_FIRST);
	assert (rc == 0);
 
	// iterate through keys 
        while ((rc = mdb_cursor_get (cursor, &mkey, &mval, MDB_NEXT)) == 0) {
               	
		// get count
		mdb_cursor_count (cursor, &count);
         	
	/*	if (count == MAX_KEY_COUNT) {
			j = sprintf(max_keys + j, "%s\n", mkey.mv_data);//write to buffer
		}*/
       	
		// print key and count
		fprintf (stdout, "%s\t%lu\n", mkey.mv_data, count);           

		// position cursor at last data item of current key
                rc = mdb_cursor_get (cursor, &mkey, &mval, MDB_LAST_DUP);
        } 
	//fprintf (stdout,"The following keys have the max ~1000 data entries: %s \n", max_keys);

	//close cursors
        mdb_cursor_close (cursor);
	mdb_cursor_close (cursor_rev);

        //commit transaction
        mdb_txn_commit (txn);

        //close environment
        mdb_env_close (env);

	return 0;
}

	
