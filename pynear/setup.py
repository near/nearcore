from setuptools import find_packages, setup

setup(
    name='near.pynear',
    version='0.1.0',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'protobuf==3.6.1',
        'requests==2.21.0',
    ],
    entry_points='''
        [console_scripts]
        pynear=near.pynear.cli:run
    ''',
)
