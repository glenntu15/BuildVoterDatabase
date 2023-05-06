import pymongo
import time
import sys
from Config import Configurations

LRECL = Configurations.LRECL 
def Find_Records_From_Keys(key_file_path_name, mycol):
    splitchar = ',' if Configurations.DATA_FORMAT == "csv" else " "
    fld_voterid = 1


    print(" Opening file: ", key_file_path_name)
    try:
        infile = open(key_file_path_name, 'r')
    except IOError:
        print("could not open ",key_file_path_name)

    nrecs = 0
    while True:

        line = infile.readline()
        if not line:
            # print(" end of file, nrecs ", nrecs, "last record ", string_parts0)
            break
        nrecs += 1
        parts = line.split(splitchar)
    
        #string_parts0 = parts[fld_voterid].replace('"','')
        #vid = string_parts0.strip()
        sid = parts[fld_voterid]
        l = len(sid)
        if (l > 7):
            vid = sid
        elif l == 7:
            vid == "0" + sid
        elif l == 6:
            vid == "00" + sid
        elif l == 5:
            vid == "000" + sid
        elif l == 4:
            vid == "0000" + sid
        else:
            vid = sid
        
        
        myquery = { "_id": vid }
        mydoc = mycol.find_one(myquery)
        if (mydoc is None):
            print(" vid ",vid,"-not found")
        #else:
            #tr = mydoc["record"]
        #if (nrecs % 2000):
            #print("vid ",vid, "r: ",str)

#--------------------------------------------------------------------------------------------
def main(args):

    myclient = pymongo.MongoClient()
    mydb = myclient["voterdatabase"]
    mycol = mydb["Voters"]
    

    #x = mycol.find_one()
    #print("returned: ",x)
    #str = x["record"]
    #print(str)
    vid = "84121144"
    myquery = { "_id": vid }
    mydoc = mycol.find_one(myquery)
    str = mydoc["record"]
    print(str)

    keyfile = "searchkeys.txt"
    key_file_path_name = Configurations.DATA_PATH + keyfile
    t1 = time.process_time()
    Find_Records_From_Keys(key_file_path_name, mycol)
    t2 = time.process_time()
    print(" time to find records: ",(t2-t1))

    

if __name__ == "__main__":
    main(sys.argv[1:])