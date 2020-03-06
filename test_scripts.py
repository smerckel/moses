import sys
sys.path.insert(0, '.')

import dbdreader
from moses import __main__

import glob

fns = dbdreader.DBDList(glob.glob('/home/lucas/gliderdata/caboverde_201911/ld/dipsy*.sbd'))
fns.sort()



sys.argv=['main',
          '--processor_directory=/home/lucas/gliderdata/caboverde_201911/coriolis/matlab',
          '--id=20191123', '--skip_ftp_transfer'] + fns
__main__.script_coriolis_upload()
