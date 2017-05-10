from setuptools import setup

setup(name='pydbvolve',
      description='Database migrations',
      long_description='Database migrations with Python3 using sql or python3 migrations. Evolve your database with python!',
      url="https://github.com/Teamworksapp/pydbvolve",
      author="Teamworksapp",
      author_email="teamworks_pypi@teamworks.com",
      version='1.0.0',
      license='MIT',
      classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Utilities',
        'Topic :: Database',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.5',
    ],
    keywords='database migration sql python python3 pydbvolve',
    packages=['pydbvolve'],
    scripts=['bin/pydbvolve'],
)

