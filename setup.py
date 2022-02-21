import pathlib

from setuptools import setup
from collections import OrderedDict

here = pathlib.Path(__file__).parent.resolve()
requirements = (here / 'requirements.txt').read_text(encoding='utf-8').split("\n")


setup(name='orapi',
      version='0.0.5',
      description='python api providing access to openresearch events and event series',
      long_description_content_type='text/markdown',
      url='https://github.com/tholzheim/orapi',
      download_url='https://github.com/tholzheim/orapi',
      author='tholzheim',
      license='Apache',
      project_urls=OrderedDict(
        (
            ("Code", "https://github.com/tholzheim/orapi"),
            ("Issue tracker", "https://github.com/tholzheim/orapi/issues"),
        )
      ),
      classifiers=[
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9'
      ],
      packages=['orapi'],
      package_data={'orapi': ['resources/templates/*.jinja','resources/templates/*.html']},
      install_requires=requirements,
      entry_points={
         'console_scripts': [
             'orapi = orapi.webserver:main',
      ],
    },
      zip_safe=False)
