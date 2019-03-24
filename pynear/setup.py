from setuptools import find_packages, setup

tests_require = [
    'delegator.py==0.1.1',
    'pytest==4.3.0',
    'retrying==1.3.3',
]

setup(
    name='near.pynear',
    version='0.1.1',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'ed25519==1.4',
        'protobuf==3.6.1',
        'requests==2.21.0',
    ],
    entry_points='''
        [console_scripts]
        pynear=near.pynear.cli:run
    ''',
    setup_requires=['pytest-runner'],
    tests_require=tests_require,
    extras_require={
        'test_utils': tests_require,
    }
)
