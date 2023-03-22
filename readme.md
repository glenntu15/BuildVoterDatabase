BuildVoterDatabase is the driver for initialization.  It has a function call copied out that process a daily voting list to produce just keys (voterid, and sosid)
it clalls functions in the Block_IO class that build blocks out of records.
The bottom later is Block_File_Interface that writes to the file -- it may be swapped out for Object_Interface (when that is written)
ReadFromDatabase is the driver file that reads the binary file, it will later read the llist of keys and retrieve records from the binary file in random order.
