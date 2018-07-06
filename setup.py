from setuptools import setup, find_packages

setup(
    name='ta3-upload',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        'agavepy',
        'boto3',
        'uri'
    ]
)