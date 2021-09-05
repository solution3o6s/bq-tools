from setuptools import setup

setup(
    name='bq_du',
    version='1.0.0',
    author='Dmitri Krasnenko',
    license='Apache License 2.0',
    description='du like command for BQ fields',
    author_email='dmitri@3o6.solutions',
    packages=[
        'bq_du'
    ],
    install_requires=[
        'google-cloud-bigquery',
    ],
    zip_safe=False,
)
