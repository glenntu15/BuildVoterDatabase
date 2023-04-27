from UsefulFunctions import *
from Config import Configurations
import time
import math
import os
import linecache

TOTAL_RECORDS = 2487525
NUM_OBJECTS   = 10
BUCKET        = Configurations.S3_BUCKET_NAME + "-" + str(NUM_OBJECTS)
USE_S3        = True
JUST_READ     = False
BUFFER_1      = True

MIN_RECORDS_PER_OBJECT, R = divmod(TOTAL_RECORDS, NUM_OBJECTS)
MAX_RECORDS_PER_OBJECT    = MIN_RECORDS_PER_OBJECT+1 if R > 1 else MIN_RECORDS_PER_OBJECT
BLOCKS_WITH_MAX_RECORDS   = R if R > 1 else NUM_OBJECTS

nrecs_write = 0
nrecs_read = 0

min_rec = math.inf
max_rec = 0
avg_rec = 0

split_chr = ","

location_dict = dict()
objects_for_s3 = dict()

build_time = 0
read_time = 0

# split_lengths = dict()
# def get_split_length(i):
#     global split_lengths
#     length = D + min(i+1, M) - min(i, M)
#     if length in split_lengths:
#         split_lengths[length] += 1
#     else:
#         print(f"new length ({length}) at {i}")
#         split_lengths[length] = 1
#     return length
# def get_split_lengths():    
#     for i in range(NUM_OBJECTS):
#         get_split_length(i)
#     print(split_lengths)

# block_rec_lengths = dict()
# def set_block_rec_length(length):
#     if length in block_rec_lengths:
#         block_rec_lengths[length] += 1
#     else:
#         print(f"new length ({length}) at {nrecs_write}")
#         block_rec_lengths[length] = 1

# def get_block_rec_lengths():
#     print(block_rec_lengths)

##-------------------------------------------------------------------------------
def keep_rec_summary_stats(rec:int):
    global nrecs_write
    global min_rec
    global max_rec
    global avg_rec

    nrecs_write += 1
    if rec < min_rec:
        min_rec = rec
    elif rec > max_rec:
        max_rec = rec
    
    a = 1 / nrecs_write
    avg_rec = a*rec + (1-a)*avg_rec

##-------------------------------------------------------------------------------
# files_s3 = []
def create_local_files_s3():
    original_dir = os.getcwd()
    files_for_s3_dir = 'objects_to_s3'
    if os.path.basename(original_dir) != files_for_s3_dir:
        os.chdir( os.path.join(original_dir, files_for_s3_dir) )

    filename = 'object'
    for i in range(NUM_OBJECTS):
        f = filename + str(i)
        try:
            open(f, 'x').close()
        except FileExistsError:
            os.remove(f)
            open(f, 'x').close()

        # files_s3.append(open(f, 'a'))
    
    print(f'Created {NUM_OBJECTS} local files')
    os.chdir(original_dir)

def compile_records_into_local_file_s3(f:str, lines:str):
    original_dir = os.getcwd()
    files_for_s3_dir = 'objects_to_s3'
    if os.path.basename(original_dir) != files_for_s3_dir:
        os.chdir( os.path.join(original_dir, files_for_s3_dir) )
    
    with open(f, 'w') as f_b:
        f_b.write(''.join([l for l in lines]))
    
    # if close:
    #     print(f"Closed file object{block_num}")
    #     files_s3[b].close()
    
    os.chdir(original_dir)

def upload_lines_s3(block_num:int, lines:str):
    global build_time

    filename = 'object'
    f = filename + str(block_num-1)

    if USE_S3:
        build_time += upload_string(f, BUCKET, ''.join([l for l in lines]))
        if block_num % max(1, NUM_OBJECTS//10) == 0:
            print(f" uploaded file: {f}")
        return

    compile_records_into_local_file_s3(f, lines)


def files_s3_summary():
    original_dir = os.getcwd()
    print(" compile_records_into_files_s3 cwd:", original_dir)
    files_for_s3_dir = 'data_s3_searched' if USE_S3 else 'objects_to_s3'
    if os.path.basename(original_dir) != files_for_s3_dir:
        os.chdir( os.path.join(original_dir, files_for_s3_dir) )
    
    file_name = 'object'
    # for i in range(NUM_OBJECTS):
    #     files_s3[i].close()
    for i in range(NUM_OBJECTS):
        f = file_name + str(i)
        with open(f, 'r') as f_i:
            print(f"File {f} | nrecs: {len(f_i.readlines())}")

    os.chdir(original_dir)
##-------------------------------------------------------------------------------

def build_s3(file:str, block_num:int, offset:int, block_length:int, buffer:list):
    global location_dict

    with open(file, "r") as f:
        lines = f.readlines()    # Skip header
        i = block_start = 1
        l = lines[i]
        while l:
            if offset == block_length:
                block_num += 1
                offset = 0
                if block_num > BLOCKS_WITH_MAX_RECORDS:
                    block_length = MIN_RECORDS_PER_OBJECT
                # set_block_rec_length(block_length)
            
            # l = l[:Configurations.LRECL]

            keep_rec_summary_stats(len(l))

            record = l.split(split_chr)
            
            location_dict[int(record[1])] = (block_num, offset)

            offset += 1
            i += 1
            if offset == block_length:
                if not JUST_READ:
                    upload_lines_s3(block_num, buffer + lines[block_start:i])
                block_start = i
                buffer = []
            if i == len(lines):
                break
            l = lines[i]
    
    buffer += lines[block_start:]
    # print("end of 1 file's build: ", block_num, offset)
    return block_num, offset, block_length, buffer

def build_voter_database_s3():
    t1 = time.process_time()
    if not USE_S3 and not JUST_READ: # Debug
        create_local_files_s3()
    block_num = 1
    offset = 0
    block_length = MAX_RECORDS_PER_OBJECT
    buffer = []
    # set_block_rec_length(block_length)
    file = Configurations.DATA_PATH + Configurations.DATA_FILE_NAME_ROOT
    for i in range(5):
        block_num, offset, block_length, buffer = build_s3(file + str(i) + ".csv", block_num, offset, block_length, buffer)
    if not (block_num == NUM_OBJECTS and offset == MIN_RECORDS_PER_OBJECT):
        error_print("Did not split correctly")
    t2 = time.process_time()
    print(f" num objects: {block_num}")
    print(" nrecs: ", nrecs_write, " (min, avg, max) record length: ", (min_rec, avg_rec, max_rec))
    print(" ====> time to build dictionary: ", (t2-t1))
    print(" ====> S3 build time: ", build_time)

##-------------------------------------------------------------------------------
read_dict = dict()
buffer_read_block_nums = []
buffer_read = ''
buffer_read_hits = 0
def read_s3(file:str):
    global nrecs_read
    global buffer_read_block_nums
    global buffer_read
    global buffer_read_hits
    global read_time

    with open(file, "r") as f:
        l = f.readline()
        while l:
            record = l.split(split_chr)

            # s3_file_name = record[1].rjust(8, "0")
            # print("File:", s3_file_name)
            # if nrecs_read >= 5:
            #     break
            
            ## Test:
            block_num, offset = location_dict[int(record[1])]
            download_dir = 'data_s3_searched/' if USE_S3 else 'objects_to_s3/'
            filename = 'object' + str(block_num - 1)
            file = download_dir + filename
            ## Download File from Bucket
            if block_num in buffer_read_block_nums:
                buffer_read_hits += 1
            else:
                if USE_S3:
                    read_time += download_file(filename, BUCKET)
                    if not BUFFER_1 and block_num % max(1, NUM_OBJECTS//10) == 0:
                        print(f" Downloaded file: {file}")
                if BUFFER_1:
                    buffer_read_block_nums.clear()
                buffer_read_block_nums.append(block_num)

            buffer_read = linecache.getline(file, offset+1)
            
            try:
                read_dict[int(record[1])] = buffer_read#[offset]
            except IndexError as e:
                print(f"Failed on rec {int(record[1])}, which had a block_num = {block_num}, offset = {offset}")
                error_print(e)

            if nrecs_read % 50000 == 0:
                print(f" Read thus far: {nrecs_read}, id: [{int(record[1])}] => {(block_num, offset)}")
                print(f"  - Current rec: {read_dict[int(record[1])]}")

            # if nrecs_read % 10000 == 0:
            #     print("Read thus far:", nrecs_read)
            
            nrecs_read += 1
            l = f.readline()
    
    print(" nrecs read: ", nrecs_read)
    print(" nrecs in read_dict:", len(read_dict))
    print(" # of hits:", buffer_read_hits)

def read_voter_database_s3():
    keyfile = "searchkeys.txt"
    keyfile = 'data/' + keyfile
    keyfile = os.path.join(os.getcwd(), keyfile)

    t1 = time.process_time()
    read_s3(keyfile)
    t2 = time.process_time()
    print(" ====> time to read records: ", (t2-t1))
    print(" ====> S3 read time: ", read_time)

##-------------------------------------------------------------------------------

def main():
    Configurations.initialize_default()
    print(BUCKET)

    ## MUST `init_boto3()` Prior to All AWS S3 Functions
    t1 = time.process_time()
    init_boto3()
    t2 = time.process_time()
    print(f" > time to init boto3: {t2-t1}")
    ## Create Bucket:
    t1 = time.process_time()
    create_bucket(BUCKET)
    t2 = time.process_time()
    print(f" > time to create bucket: {t2-t1}")

    t1 = time.time()
    build_voter_database_s3()
    t2 = time.time()
    print(" nrecs in location_dict:", len(location_dict))
    print("  Actual total time to Build:", t2-t1)
    # get_block_rec_lengths()
    # get_split_lengths()
    t1 = time.time()
    read_voter_database_s3()
    t2 = time.time()
    print("  Actual total time to Read:", t2-t1)
    
    # files_s3_summary()


if __name__ == "__main__":
    main()