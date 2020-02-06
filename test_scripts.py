import sys
sys.path.insert(0, '.')

from moses import __main__


sys.argv=['main',
          '--processor_directory=/home/lucas/gliderdata/caboverde_201911/coriolis/matlab',
          '--id=20191123', '--skip_ftp_transfer',
          '/home/lucas/gliderdata/caboverde_201911/ld/sebastian-2019-327-04-000.sbd']
__main__.script_coriolis_upload()
