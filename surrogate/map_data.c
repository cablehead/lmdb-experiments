/*
 * File Name: 	map_data.c
 * Name: 	Mariam Issa
 * Function: 	This function is designed to take in a stream
 *		of JSON data that is already formatted in the
 * 		following form: <url> <key> ... <key> on each
 *		line of the file. This file will then hash 
 *		each key with its respective URL and store it
 * 		in a data store, created by a Lightning Based
 * 		Memory-mapped Data Base, with the surrogate
 *		key being the key in the data store and the 
 * 		URL as the mapped value for this key. Simul-
 * 		taneously, a separate reverse-mapped data-
 * 		base will be created with the URL as the key
 * 		with its stored value(s) being each key that
 * 		is mapped to it.   
 */

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include "lmdb.h"
#include "blake2/sse/blake2.h"
#include "blake2/sse/blake2-impl.h"

const int NUM_BYTES = 17;
const int HASH_BYTES = 8;
const int COMMIT_TXN = 10000;
const size_t MAX_KEY_COUNT= 100000;
const unsigned int FLAGS = MDB_DUPSORT |  MDB_DUPFIXED | MDB_CREATE;


int byte_to_hex (char outstr[], char *  instr, size_t count);


int
main(int argc, char * argv[]) {
    
	// set up variables
        int rc, keys_added, duplicates, lines_read; 
	size_t count, max_count;
	char  blake_str [HASH_BYTES];

	int lines = 0;
	clock_t begin = clock();
	clock_t end;
	double time_spent;

	MDB_env *env;
        MDB_dbi dbi, dbi_rev;
        MDB_txn *txn;
        MDB_cursor *cursor;
	MDB_stat stats; //store stats for # of data items

        // set up key and node info
        MDB_val mkey, mval, tmp_val;
        char key [NUM_BYTES]; 
        char val [NUM_BYTES];
        
	mkey.mv_size = NUM_BYTES;
        mkey.mv_data = &key;
        mval.mv_size = NUM_BYTES; 
        mval.mv_data = &val;

        // initialize environment; set 2 database limit
        rc = mdb_env_create (&env);
        assert (rc == 0);
	rc = mdb_env_set_mapsize (env, 1.5*1024*1024*1024);
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

	// set up for hashing
        size_t j;
        char line [500];
        char * token;
  
	uint8_t hash_key[HASH_BYTES];
        for( j = 0; j < HASH_BYTES; ++j ) {       
                hash_key[j] = ( uint8_t )j;
        }

	// process each line
	while ( fgets (line, 500, stdin) != NULL ) {       

                token = strtok (line, " ");  //gets url

                // hash URL        
                rc = blake2b (blake_str, HASH_BYTES, token, strlen (token) , hash_key, HASH_BYTES);
                assert (rc == 0);

		// print bytes out in hex
		rc = byte_to_hex (val, blake_str, sizeof(blake_str));
		assert (rc == 0);
		
               // process each key 
                while ((token = strtok (NULL, " ")) != NULL) {     

			// hash key 
                        rc = blake2b (blake_str, HASH_BYTES, token, strlen (token), hash_key, HASH_BYTES);
                        assert (rc == 0); 
              	 	
			// print bytes out in hex
	                rc = byte_to_hex (key, blake_str, sizeof(blake_str));
			assert (rc == 0);
			
			// check if key exists and has too many data entries
			if ( mdb_cursor_get (cursor, &mkey, &tmp_val, MDB_SET) == 0) {
				mdb_cursor_count (cursor, &count);
				if (count >= MAX_KEY_COUNT) {
					continue;
				}
			}
			// enter in database
                	rc = mdb_cursor_put (cursor, &mkey, &mval, MDB_NODUPDATA);
                	
			// track number of duplicates	
			if (rc == MDB_KEYEXIST) {
				duplicates++;	
				continue;
			}
			else if ( rc == 0) {
				keys_added++;
			}	
			else {  
                                fprintf (stderr, "Failure to add key into database");
                                return -1;
                        }
			
			// track count of items at current key
			rc = mdb_cursor_count (cursor, &count);
		        assert (rc == 0);
		
			//fprintf (stdout, "Key: %s\t\t\t Count: %zu \n",  mkey.mv_data, count);
	
			// enter in reverse-mapped database
               		rc = mdb_put ( txn, dbi_rev, &mval, &mkey, 0); 
                	assert (rc == 0);
			
		}
		
		// track lines read
                lines_read++;  
		
		if ((lines_read % COMMIT_TXN) == 0) {
                	//commit transaction
                	rc = mdb_txn_commit (txn);
                	assert (rc == 0);

                	// reset transaction
                	rc = mdb_txn_begin (env, NULL, 0, &txn);
                	assert (rc == 0);
		
                	// re-initiate cursor
                	rc = mdb_cursor_open (txn, dbi, &cursor);
                	assert (rc == 0);
          	}

		lines += 1;
		if (lines % 100000 == 0) {
			end = clock();
			time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
			begin = end;
			fprintf (stdout, "%d %f\n", lines, time_spent);
		}

		// reset buffers
		memset(blake_str, 0, sizeof(blake_str));
		memset (key, 0, sizeof(key));
		memset (val, 0, sizeof(val));
	}
	
	fprintf (stdout, "\nAdded a total of %d keys to the data store \n", keys_added);
	fprintf (stdout, "There were %d duplicates \n", duplicates); 
	
	//close cursor
	mdb_cursor_close (cursor);
	
	//commit transaction
	mdb_txn_commit (txn); 
	
	//close environment
        mdb_env_close (env);

        return 0;
}


int byte_to_hex (char outstr[], char * instr, size_t count) {
	int i;
	for (i = 0; i < count; i++) {
		sprintf (&outstr[2 * i], "%02X",  instr[i]);
	}
	outstr[16] = '\0';
	return 0;
}

