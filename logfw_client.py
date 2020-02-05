import moses.filetransport as filetransport
pub_port = 7010
req_port = 7011
server='localhost'
datadir = 'hzg_gliders'
client = filetransport.FileForwarderClient(datadir=datadir,
                                           processor_coro = None,
                                           force_reread_all=True,
                                           sub_dir='logs')
client.add_server(server, pub_port, req_port)
client.connect_all()
client.run()
