from setuptools import setup

install_requires = [
    'requests',
]

tests_require = [
    'mock',
    'nose',
]

setup(
    name="salesforce_client",
    version="0.1",
    packages=['salesforce_client'],
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite="nose.collector",
)
