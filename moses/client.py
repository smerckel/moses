import filetransport
import asciiwriter


writer = asciiwriter.MosesDBDWriter(output_directory='/home/lucas/even/fw/processed')

processor = asciiwriter.processor(writer, extensions=('sbd', 'tbd'))

client = filetransport.FileForwarderClient(datadir='/home/lucas/even/fw/working_dir', processor_coro = processor,
                                           force_reread_all=True)

client.add_server("localhost", 8000, 8001)

client.setup_connections()
client.connect()

client.run()
