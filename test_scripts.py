import sys
sys.path.insert(0, '.')

import dbdreader
from moses import __main__

import glob

if 0: # test 0
    fns = dbdreader.DBDList(glob.glob('/home/lucas/gliderdata/caboverde_201911/ld/dipsy*.sbd'))
    fns.sort()



    sys.argv=['main',
              '--processor_directory=/home/lucas/gliderdata/caboverde_201911/coriolis/matlab',
              '--id=20191123', '--skip_ftp_transfer'] + fns
    __main__.script_coriolis_upload()


if 1:
    fns = dbdreader.DBDList(glob.glob('/home/lucas/gliderdata/caboverde_201911/hd/amadeus*.dbd'))
    fns.sort()



    sys.argv=['main',
              '--processor_directory=/home/lucas/gliderdata/caboverde_201911/coriolis/matlab'] + fns
    __main__.script_convert_for_coriolis()
