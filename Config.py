from UsefulFunctions import error_print, is_pow_of_2

class DefaultConfigurations:
    BLOCKSIZE = 4096
    LRECL = 128
    DRECL = 16

    VOTERS_TO_PROC = 500000
    
    #DATA_PATH = "C:\\Users\\dfern\\OneDrive\\Documents\\UH\\Senior\\Spring2023\\COSC6376\\code\\tmp\\"
    DATA_PATH = "C:/tmp/"
    DATA_FORMAT = "csv"
    DATA_ATTRIBUTES = ["placeholder"]   # DF: Add header here
    DATA_FILE_NAME_ROOT = "registered_voters" # Might also be "test_file"
    optimize_reads = False
    DATABASE_PATH = "./"
    DATABASE_NAME ="database.bin"

class Configurations:
    BLOCKSIZE = DefaultConfigurations.BLOCKSIZE
    LRECL = DefaultConfigurations.LRECL
    DRECL = DefaultConfigurations.DRECL
    VOTERS_TO_PROC = DefaultConfigurations.VOTERS_TO_PROC

    DATA_PATH = DefaultConfigurations.DATA_PATH
    DATA_FORMAT = DefaultConfigurations.DATA_FORMAT
    DATA_ATTRIBUTES = DefaultConfigurations.DATA_ATTRIBUTES
    DATABASE_PATH = DefaultConfigurations.DATABASE_PATH
    DATABASE_NAME = DefaultConfigurations.DATABASE_NAME
    optimize_reads = DefaultConfigurations.optimize_reads
    
    _CONFIGURABLES = [i for i in dir(DefaultConfigurations) if i[0].isupper()]

    def initialize_default():
        Configurations.BLOCKSIZE = DefaultConfigurations.BLOCKSIZE
        Configurations.LRECL = DefaultConfigurations.LRECL
        Configurations.DRECL = DefaultConfigurations.DRECL
        Configurations.VOTERS_TO_PROC = DefaultConfigurations.VOTERS_TO_PROC
        Configurations.DATA_PATH = DefaultConfigurations.DATA_PATH
        Configurations.DATA_FORMAT = DefaultConfigurations.DATA_FORMAT
        Configurations.DATA_ATTRIBUTES = DefaultConfigurations.DATA_ATTRIBUTES
        Configurations.DATA_FILE_NAME_ROOT = DefaultConfigurations.DATA_FILE_NAME_ROOT
        Configurations.DATABASE_PATH = DefaultConfigurations.DATABASE_PATH
       
        Configurations.optimize_reads = False
        Configurations.verify()

    def initialize(blocksize = None, lrecl = None, dict_recl = None, voters_to_proc = None,
                   data_path = None, data_format = None, data_header = None, data_file_name_root = None, 
                   database_path = None, opt_reads = None):
        Configurations.BLOCKSIZE = blocksize if blocksize is not None else DefaultConfigurations.BLOCKSIZE
        Configurations.LRECL = lrecl if lrecl is not None else DefaultConfigurations.LRECL
        Configurations.DRECL = dict_recl if dict_recl is not None else DefaultConfigurations.DRECL
        Configurations.VOTERS_TO_PROC = voters_to_proc if voters_to_proc is not None else DefaultConfigurations.VOTERS_TO_PROC
        Configurations.DATA_PATH = data_path if data_path is not None else DefaultConfigurations.DATA_PATH
        Configurations.DATA_FORMAT = data_format if data_format is not None else DefaultConfigurations.DATA_FORMAT
        Configurations.DATA_ATTRIBUTES = data_header if data_header is not None else DefaultConfigurations.DATA_ATTRIBUTES
        Configurations.DATA_FILE_NAME_ROOT = data_file_name_root if data_file_name_root is not None else DefaultConfigurations.DATA_FILE_NAME_ROOT
        Configurations.DATABASE_PATH = database_path if database_path is not None else DefaultConfigurations.DATABASE_PATH

        Configurations.optimize_reads = opt_reads if opt_reads is not None else DefaultConfigurations.opt_reads

        Configurations.verify()

    def initialize_sizes(blocksize, lrecl, dict_recl):
        Configurations.initialize(blocksize, lrecl, dict_recl)
    
    def initialize_data_info(data_path, data_format, data_header):
        Configurations.initialize(None, None, None, None, data_path, data_format, data_header)

    def verify():
        if Configurations._CONFIGURABLES != [i for i in dir(Configurations) if i[0].isupper()]:
            error_print("`Configurations` and `DefaultConfigurations` must have the same configurables")

        for i in Configurations._CONFIGURABLES:
            if "_" not in i: # Get the sizes/length configurables
                if not is_pow_of_2(getattr(Configurations, i)):
                    error_print(f"`{i}` must be an number that is a power of 2")

        # DF: `< or <=`?
        if Configurations.LRECL > Configurations.BLOCKSIZE:
            error_print("`LRECL` must be smaller than or equal to `BLOCKSIZE`")
        if Configurations.DRECL > Configurations.LRECL:
            error_print("`DRECL` must be smaller than or equal to `LRECL`")
        
        if not (type(Configurations.VOTERS_TO_PROC) == int
                and Configurations.VOTERS_TO_PROC > 0):
            error_print("`REGISTERED_VOTERS_TO_PROCESS` must be an integer > 0")
        
        if not (type(Configurations.DATA_PATH) == str and len(Configurations.DATA_PATH) > 0):
            error_print("`DATA_PATH` must be a non-empty string")
        if not (type(Configurations.DATA_FORMAT) == str and len(Configurations.DATA_FORMAT) > 0):
            error_print("`DATA_FORMAT` must be a non-empty string")
        if not (type(Configurations.DATA_ATTRIBUTES) == list and len(Configurations.DATA_ATTRIBUTES) > 0
                and all(type(i)==str for i in Configurations.DATA_ATTRIBUTES)):
            error_print("`DATA_HEADER` must be a non-empty list of strings")

    def display():
        print("Configurations:")
        for i in Configurations._CONFIGURABLES:
            print(f" - {i}: {getattr(Configurations, i)}")
        print()
        

def main():
    Configurations.initialize_default()
    Configurations.display()

    Configurations.initialize_sizes(256,128,16)
    Configurations.display()

    Configurations.initialize_data_info("C:/", "json", ["id", "info"])
    Configurations.display()

    print("\nSuccessfully set all Configurations!\n")

if __name__ == "__main__":
    main()