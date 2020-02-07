from setuptools import setup

setup(name='datatc',
      version='0.0.1',
      description='',
      url='https://github.com/uzh-dqbm-cmi/data-traffic-control',
      packages=['datatc',],
      python_requires='>3.5.0',
      install_requires=[
            'flake8',
            'gitpython',
            'pandas',
            'pymupdf',
            'pyyaml',
      ],
      zip_safe=False)
