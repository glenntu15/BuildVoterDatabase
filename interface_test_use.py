from interface_test import interface_test
from interface_child_test import *
from UsefulFunctions import error_print

def verify_type(obj):
    print("Verifying Object type:")
    if isinstance(obj, interface_test):
        # if type(obj)==s3_test:
        # if type(obj)==ebs_test:
        print(f" ✓ `{obj.name}` of type {type(obj)} is an instance of `interface_test`")
        print(f"")
    else:
        print(f" 🗴 Object given is not an instance of `interface_test`")

def use_all_functions(bs:interface_test):
    print("Attempt to use common function:", end="\n ")
    bs.function_must_be_defined_by_all()

    try:
        print("Attempt to use EBS:", end="\n ")
        bs.use_ebs()
    except NotImplementedError as e:
        error_print(e, False)
    
    try:
        print("Attempt to use S3:", end="\n ")
        bs.use_s3()
    except NotImplementedError as e:
        error_print(e, False)
    print()

for i in range(3):
    try:
        bs = interface_test() if i==0 else ebs_test() if i==1 else s3_test()
        verify_type(bs)
        use_all_functions(bs)
    except TypeError as e:
        print("Init was Not Successful", end="\n ")
        error_print(e, False)
        print()