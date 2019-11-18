import filetransport

FW = filetransport.FileForwarder((8000, 8001), '/home/lucas/even/fw/from-glider', info_interval=5)
FW.run()
