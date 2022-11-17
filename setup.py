from setuptools import setup, find_packages
import os

package_data = {
        "grocery": [os.path.join('config', x) for x in os.listdir('grocery/config/')] # Grab anything in the config directory
}

setup(
    name='grocery',
    url='https://test',
    author='Zach Polke',
    author_email='zach.polke@hey.com',
    packages=find_packages(),
    version='0.0.1',
    license='NA',
    description='Grocery Retail',
    package_data=package_data,      # New line
)