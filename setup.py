from setuptools import setup

setup(name="moses",
      version="0.0.1",
      packages = ['moses'],
#      py_modules = ['fastachar'],
#      entry_points = {'console_scripts':[],#['fastachar_gui = fastachar_gui:main'],
#                      'gui_scripts':['fastachar = fastachar:main']
#                      },
      install_requires = 'numpy dbdreader timeconversion zmq'.split(),
      author="Lucas Merckelbach",
      author_email="lucas.merckelbach@hzg.de",
      description="",
      long_description="""""",
    url='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: MacOS :: MacOS X',
                  'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
    ])
