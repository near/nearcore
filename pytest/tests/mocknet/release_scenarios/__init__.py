from .test27 import Test27, Test27Small

# Dictionary mapping test case names to their classes
TEST_CASES = {
    'test2.7': Test27,
    'test2.7-small': Test27Small,
    # Add more test cases here as needed
}


def get_test_case(test_case_name, args):
    """
    Get a test case instance by name.

    Args:
        test_case_name (str): Name of the test case
        args: Arguments object to pass to the test case constructor

    Returns:
        TestSetup: Instance of the requested test case
    """
    if test_case_name not in TEST_CASES:
        available_cases = list(TEST_CASES.keys())
        raise ValueError(
            f"Unknown test case: {test_case_name}. Available test cases: {available_cases}"
        )

    test_class = TEST_CASES[test_case_name]
    return test_class(args)


def get_available_test_cases():
    """Get a list of all available test case names."""
    return list(TEST_CASES.keys())
