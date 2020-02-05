import moses.filetransport as filetransport
pub_port = 7010
req_port = 7011
directory = ['/home/localuser/gliders/amadeus/logs',
             '/home/localuser/gliders/sebastian/logs',
             '/home/localuser/gliders/comet/logs',
             '/home/localuser/gliders/dipsy/logs']
info_interval=30
regex_pattern=".*log"

FW = filetransport.FileForwarder((pub_port, req_port), *directory, info_interval=info_interval, regex_pattern=regex_pattern)
FW.run()
