from .base import TestSetup
import os
import importlib


def get_available_test_cases_impl():
    """
    Get all test cases that inherit from TestSetup.
    """
    test_cases = {}
    for file in os.listdir(os.path.dirname(__file__)):
        if file.endswith('.py') and file != '__init__.py':
            module_name = file[:-3]
            module = importlib.import_module(f'{__name__}.{module_name}')
            for name, obj in module.__dict__.items():
                if isinstance(obj, type) and obj != TestSetup and issubclass(
                        obj, TestSetup):
                    test_cases[name] = obj
    return test_cases


def get_test_case(test_case_name, args):
    """
    Get a test case instance by name.

    Args:
        test_case_name (str): Name of the test case
        args: Arguments object to pass to the test case constructor

    Returns:
        TestSetup: Instance of the requested test case
    """
    test_cases = get_available_test_cases_impl()
    if test_case_name not in test_cases:
        available_cases = list(test_cases.keys())
        raise ValueError(
            f"Unknown test case: {test_case_name}. Available test cases: {available_cases}"
        )

    test_class = test_cases[test_case_name]
    return test_class(args)


def get_available_test_cases():
    """Get a list of all available test case names."""
    return list(get_available_test_cases_impl().keys())
