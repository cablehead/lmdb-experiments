/*
 * File Name: 	purge.c 
 * Name: 	Mariam Issa
 * Function:	This function will look up a key for an image 
 *		in the created data store (mapped by map_data.c) 
 *      	and purge the data store of all other keys that 
 *	     	share the same URL as the tainted key.  It will 
 *	     	also return the count of they key.
 * Usage:	After succesfful compilation, this program is run
 * 	  	by entering ./purge <key> or either ./purge, in 
 * 	  	which the program will request the entry of the 
 *	  	key that needs to be deleted.
 */

#include <stdlib.h>
#include <sys/errno.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "lmdb.h"
#include "blake2/sse/blake2.h"
#include "blake2/sse/blake2-impl.h"

const unsigned int FLAGS = MDB_DUPSORT | MDB_DUPFIXED;
const int NUM_BYTES = 17;
const int HASH_BYTES = 8;
int byte_to_hex (char outstr[], char * instr);

int
main(int argc, char * argv[]) {

	// set up variables
	int rc, i = 0, j, image_number = 0, key_number = 0; 
	size_t num_images = 0, num_url_keys = 0;
	char blake_str [HASH_BYTES];
	char hashed_key [NUM_BYTES];
	char url_array [NUM_BYTES];
	char * key_to_delete;
	char * hash_status;

	// database variables
	MDB_env *env;
        MDB_dbi dbi, dbi_rev;
        MDB_txn *txn;
        MDB_cursor *cursor, *cursor2, *cursor_rev;
	MDB_val key, url, new_key;
	MDB_stat stats;	
	
        //set up key to look up
        key.mv_size = NUM_BYTES;
        key.mv_data = &hashed_key;
        url.mv_size = NUM_BYTES;
	url.mv_data = &url_array;

	// set up key 
        uint8_t hash_key[HASH_BYTES];
        for( j = 0; j < HASH_BYTES; ++j ) {   
                hash_key[j] = (uint8_t) j;
        }   
	
	// assign search key
	if (argc == 1) {   
                fprintf (stdout, "Enter in key to delete: "); 
                scanf ("%s", key_to_delete);
	}   
        else {   
                strncpy (key_to_delete, argv[1], strlen(argv[1]));
        }   

	// get hash status	
	fprintf (stdout, "Is this key hashed(yes/no)?\n");
	scanf ("%s", hash_status);
 
	if ((strcmp (hash_status, "no")) == 0) {
        	// hash input string to key 
		rc = blake2b (blake_str, HASH_BYTES, key_to_delete, strlen(key_to_delete), hash_key, HASH_BYTES);
        	assert (rc == 0); 
   
		rc = byte_to_hex (hashed_key, blake_str);
		assert (rc == 0);
	}
	else if ((strcmp(hash_status, "yes")) == 0) {
		strncpy(hashed_key, key_to_delete, NUM_BYTES);	
	}
	else {
		fprintf (stderr, "INVAlID INPUT \n Exiting Program...\n");
		return -1;
	}	
	
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
	assert (rc == 0);

	// initiate cursors
	rc = mdb_cursor_open (txn, dbi, &cursor);
	assert (rc == 0);
	rc = mdb_cursor_open (txn, dbi_rev, &cursor_rev);
	assert (rc == 0);
	rc = mdb_cursor_open (txn, dbi, &cursor2);
        assert (rc == 0);

		 /***End of Set Up***/

	// locate key, get first url
	rc = mdb_cursor_get (cursor, &key, &url, MDB_SET | MDB_FIRST_DUP); 
	if (rc != 0) {
		fprintf (stderr, "\nERROR: Key not found\n\n");
		return -1;
	}

	
	
	// count, print # of images(URLs) under this input key 
        rc = mdb_cursor_count (cursor, &num_images);
        assert (rc == 0);
	fprintf (stdout, "\nThis key has %zu image(s)\n", num_images); 
	
	while ( image_number < num_images ) {
	
                // get 1st key from reverse mapped database
		rc = mdb_cursor_get (cursor_rev, &url, &new_key, MDB_SET_KEY | MDB_FIRST_DUP); 
		if (rc != 0) {
        	        fprintf (stderr, "\nERROR: Key's URL not found\n\n");
        	        return -1;
       		}

		// get,print count of keys under URL
		rc = mdb_cursor_count (cursor_rev, &num_url_keys);
		assert (rc == 0);
		fprintf (stdout, "\nImage %d has %zu keys\n", (image_number+1), num_url_keys);

		// loop through to delete each key
		while (key_number < num_url_keys) {

			//position at specified key and url
               		rc = mdb_cursor_get (cursor2, &new_key, &url, MDB_GET_BOTH);
                        if (rc == 0) {


				fprintf (stdout, "Deleting from %s: Key %s \tURL %s\n", key.mv_data, new_key.mv_data, url.mv_data);
				rc = mdb_cursor_del (cursor2, 0); 
                                assert (rc == 0);
				i++; //delete total number of items deleted
			}
			else 
				fprintf (stderr, "ERROR: Finding the folowing surrogate key: %s\n", new_key.mv_data);
		
			// get next key in reverse mapped database
			if ((mdb_cursor_get (cursor_rev, &url, &new_key, MDB_NEXT_DUP)) == 0) {
				key_number++;
			}
			else {
				// delete URL in reverse mapped data map and all its entries
        		        rc = mdb_cursor_del (cursor_rev, MDB_NODUPDATA);
        		        assert (rc == 0);
				break;
			}
		}
		//commit transaction 
                rc = mdb_txn_commit (txn);
                assert (rc == 0);

                // reset transaction
                rc = mdb_txn_begin (env, NULL, 0, &txn);
                assert (rc == 0);

                // re-initiate cursor
                rc = mdb_cursor_open (txn, dbi, &cursor);
                rc = mdb_cursor_open (txn, dbi_rev, &cursor_rev);
		rc = mdb_cursor_open (txn, dbi, &cursor2);
		assert (rc == 0);
		
		// get next image
		if ((mdb_cursor_get (cursor, &key, &url, MDB_SET | MDB_NEXT_DUP)) == 0) {
			image_number++;			
			key_number = 0;
		}
		else {
			break;
		}
	}
	
	// print total number of items deleted
	fprintf (stdout,"%d instances of %s deleted from data store\n\n", i, key_to_delete);

	// close cursors
	mdb_cursor_close (cursor);
	mdb_cursor_close (cursor_rev);
	mdb_cursor_close (cursor2);

	// close transaction
	mdb_txn_commit (txn);

	//close environment
	mdb_env_close (env);

	return 0;
	
}

int byte_to_hex (char outstr[], char * instr) {
        int i;
        for (i = 0; i < sizeof (instr); i++) {
                sprintf (&outstr[2 * i], "%02X",  instr[i]);
        }
	outstr [(NUM_BYTES - 1)] = '\0';
        return 0;
}
