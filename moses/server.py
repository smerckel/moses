import filetransport

FW = filetransport.FileForwarder((7000, 7001), '/home/lucas/even/fw/from-glider', info_interval=10)
FW.run()
