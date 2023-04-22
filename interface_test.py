import abc

class interface_test(object, metaclass=abc.ABCMeta):
    def __init__(self):
        self.name = "interface_test"
        print("Init was Successful")

    def use_ebs(self):
        raise NotImplementedError('`use_ebs()` was not defined')

    def use_s3(self):
        raise NotImplementedError('`use_s3()` was not defined')

    @abc.abstractmethod
    def function_must_be_defined_by_all(self):
        print("✓ SUCCESS: `function_must_be_defined_by_all` is defined")
