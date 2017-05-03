from setuptools import setup

setup(name='pydbvolve',
      description='Python DB Evolution',
      long_description='pydbvolve: Database migrations with Python3',
      version='1.0.0',
      license='MIT',
      classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: DevOps',
        'Topic :: Software Development :: Utilities',

        # Pick your license as you wish (should match "license" above)
        'License :: MIT :: MIT',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.5',
    ],
    keywords='Database, Migration, SQL, Python, Python3',
    packages=['pydbvolve'],
    scripts=['bin/pydbvolve'],
)

