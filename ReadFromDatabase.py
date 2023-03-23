from Block_IO import Block_IO
import sys
import time

BLOCKSIZE=4096
LRECL = 128

buffer = bytearray(4096) 

def Find_Records_From_Keys(bio, registered_voters,key_file_path_name):

    

    splitchar = ','
    fld_voterid = 1
    fld_sosid = 2

    
   
    # try reading the first data record
    #record = bio.read_spcific_record(0,0,LRECL)
    idnumber = 41926668
    result = registered_voters[idnumber]
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    record = bio.read_spcific_record(result[1],result[2],LRECL)
    testdata = record[0:100]
    sdata = testdata.decode("'utf-8'")
    print(" debug first entry: ",sdata)
    # now try the 20 th record
    idnumber = 36531523
    result = registered_voters[idnumber]
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    record = bio.read_spcific_record(result[1],result[2],LRECL)
    testdata = record[0:100]
    sdata = testdata.decode("'utf-8'")
    print(" debug 20th entry: ",sdata)
    # now try the 33 entry -- next block
    idnumber =80109549
    result = registered_voters[idnumber]
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    record = bio.read_spcific_record(result[1],result[2],LRECL)
    testdata = record[0:100]
    sdata = testdata.decode("'utf-8'")
    print(" debug 33rd entry: ",sdata)


    #testdata = record[0:100]
    #print(" debug testdata: ",testdata[0:20])
    #sdata = testdata.decode("'utf-8'")
    #print(sdata)

    print(" Opening file: ",key_file_path_name)
    try:
        infile = open(key_file_path_name, 'r')
    except IOError:
        print(" could not open file")
        exit(1)

    nrecs = 0

    while True:

        line = infile.readline();
        if not line:
            #print(" end of file, nrecs ",nrecs,"last record ",string_parts0)
            break
        #if (l > LRECL):
        #    print(" record is too long, record is: ",line);

        nrecs = nrecs + 1
        
        parts = line.split(splitchar)
        
        string_parts0 = parts[fld_voterid].replace('"','')
        string_parts1 = parts[fld_sosid].replace('"','')
        idnumber = int(string_parts0)

        result = registered_voters[idnumber]
        print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
        record = bio.read_spcific_record(result[1],result[2],LRECL)
        
        testdata = record[0:100]
        sdata = testdata.decode("'utf-8'")
        print(sdata)

        if (nrecs > 3):
            break
#----------------------------------------------------------------------------------
# Main starts here
#----------------------------------------------------------------------------------

bio = Block_IO()            # can only be one instance of bio

dbfile = "database.bin"

data_location = "c:/tmp/"
keyfile = "searchkeys.txt"

key_file_path_name = data_location + keyfile

registered_voters = {}

t1 = time.process_time()
bio.build_dictionary_from_file(dbfile, registered_voters)
t2 = time.process_time()
print(" time to build dictionary from file: ",(t2-t1))


print(" dictionary of registered voters length = ",len(registered_voters))
print(" blocks read: ",bio.return_block_count())

#count = 0
#for vid, result in registered_voters.items():
#    print(" vid: ", vid, " result",result)
#    count = count + 1
#    if (count > 10):
#        break

Find_Records_From_Keys(bio, registered_voters, key_file_path_name)
