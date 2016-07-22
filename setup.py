from setuptools import setup, find_packages
import codecs  # To use a consistent encoding

# Get the long description from the relevant file
with codecs.open('DESCRIPTION.rst', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='azure_flocker_driver',
    version='1.0',
    description='EMC Azure Backend Plugin for ClusterHQ/Flocker ',
    long_description=long_description,
    author='Steven Edouard',
    author_email='sedouard@microsoft.com',
    url='https://github.com/sedouard/azure-flocker-driver',
    license='Apache 2.0',

    classifiers=[

        'Development Status :: Alpha',

        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: MIT',

        # Python versions supported
        'Programming Language :: Python :: 2.7',
    ],

    keywords='backend, plugin, flocker, docker, python',
    packages=find_packages(exclude=['test*']),
    install_requires=['azure', 'bitmath', 'eliot'],
    data_files=[('/etc/flocker/', ['DESCRIPTION.rst']),
                ('/etc/flocker/', ['azure_storage_test.yml'])]
)
