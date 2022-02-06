from setuptools import setup

requirements_list = [line.strip() for line in open('requirements.txt').readlines()]

setup(
   name='seekanalytics',
   version='1.0.0',
   description='Job data analytics',
   author='Sury Soni',
   author_email='83525+spsoni@users.noreply.github.com',
   packages=['seekanalytics'],
   install_requires=requirements_list,
   include_package_data=True
)
