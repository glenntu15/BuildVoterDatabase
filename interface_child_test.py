from interface_test import interface_test

class ebs_test(interface_test):
    def __init__(self):
        super().__init__()
        self.name = "EBS test object"

    def use_ebs(self):
        print("✓ SUCCESS: `use_ebs()` is defined")
    
    def function_must_be_defined_by_all(self):
        return super().function_must_be_defined_by_all()

class s3_test(interface_test):
    def __init__(self):
        super().__init__()
        self.name = "S3 test object"

    def use_s3(self):
        print("✓ SUCCESS: `use_s3()` is defined")
    
    def function_must_be_defined_by_all(self):
        return super().function_must_be_defined_by_all()