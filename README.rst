MOSES
=====

Still to write.

.. note::
  Although setuptools will resolve the dependency on numpy correctly,
  it is not capable of installing numpy as a wheel, but rather
  compiles it. This may take a long time, and possibly not be
  successful because of other dependencies that are not met. Therefore
  it is recommended to install numpy either via the distribution's
  package manager, or using pip::

    $ pip3 install numpy

  which *does* install numpy's wheel version.

.. note::
   To avoid passwords to FTP sites ending up in the git repo, we now
   have to supply a password upon starting up `moses_dbd_client`. A
   hashed version is submitted to the Ifremer FTP site.

   The command `moses_hashed_password` displays the hashed version of
   the given password.
  
Authors
~~~~~~~

* Lucas Merckelbach (lucas.merckelbach at hereon.de)


The software is released under the GPLv3 licence.
