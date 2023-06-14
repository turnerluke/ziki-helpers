from setuptools import setup, find_packages

# Get requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='ziki_helpers',
    version='1.0',
    author='Turner Luke',
    author_email='turnermluke@gmail.com',
    description='Helper functions shared between multiple ZIKI projects.',
    url='https://github.com/turnerluke/ziki-helpers',
    # Commented as it's more readable to import from the root
    # ie: from ziki_helpers.service.service import fxn

    # packages=find_packages('ziki_helpers'),
    # package_dir={'': 'ziki_helpers'},
    # test_suite='tests',

    # Install dependencies
    install_requires=requirements,

)
