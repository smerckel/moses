import argparse
import sys

from . import filetransport
from . import asciiwriter

def script_moses_dbd_server():
    description='''
Glider DBD file Publisher

A server program that publishes DBD files as soon as they are written by the dockserver.
At regular intervals, the program submits the number of files transferred, so that a client
can figure out if there are any missing and fetch those on request.

'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--pub_port', type=int, help='Port number used for publishing data', default=7000)
    parser.add_argument('--req_port', type=int, help='Port number used for accepting queries from clients', default=7001)
    parser.add_argument('--interval', type=int, help='Publishing interval of number files sent in seconds', default=600)
    parser.add_argument('directory', help='Directory to monitor (supply as many as needed)', nargs='+')
    args = parser.parse_args()

    pub_port = args.pub_port
    req_port = args.req_port
    info_interval = args.interval
    directory = args.directory
    FW = filetransport.FileForwarder((pub_port, req_port), *directory, info_interval=info_interval)
    #FW = filetransport.FileForwarder((8000, 8001), '/home/lucas/even/fw/from-glider', info_interval=5)
    #FW = filetransport.FileForwarder((pub_port, req_port), , info_interval=info_interval)

    FW.run()
    

def script_moses_dbd_client():
    description='''
Glider DBD file Subscriber

A client program that subscribes to the Glider DBD file Publisher.
This allows the contents of the from-glider directory to be mirrored, with no substantial latency.
Optional the dbd data can be processed.

'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('dbd_directory', help='Directory where dbd files are written into')
    parser.add_argument('--pub_port', type=int, help='(Common) port number used for publishing data', default=7000)
    parser.add_argument('--req_port', type=int, help='(Common) port number used for accepting queries from clients', default=7001)

    parser.add_argument('--server', '-s', help='Publishing server to connect to. Format is server (using common port numbers, or server:pub_port:req_port to use specific port numbers.', action='append')
    parser.add_argument('--force-reread', action='store_true', help='Force rereading all dbd files when the client is started. If not specified, the client will not attempt to retrieve any files that are reported missing at start up.')
    parser.add_argument('--processor', help=''''Post-process data. Options are 
  * "ascii", which converts the dbd files into ascii files
  * "coriolis", which pushes the data to the coriolis ftp site.''', choices=['ascii','coriolis'], nargs='+')
    parser.add_argument('--ascii_processor_directory', help='Directory for the ascii processor to write the converted data files to')
    
    args = parser.parse_args()
    server = args.server
    dbd_directory = args.dbd_directory
    processor = args.processor
    force_reread = args.force_reread
    ascii_directory = args.ascii_processor_directory
    pub_port = args.pub_port
    req_port = args.req_port
    writer = None # unless otherwise determined.
    if processor and len(processor)==2:
        raise NotImplementedError('Todo, make a fanned pipeline')
    elif processor and processor[0] == 'ascii':
        writer = asciiwriter.MosesDBDWriter(output_directory=ascii_directory)
        processor = asciiwriter.processor(writer, extensions=('sbd', 'tbd'))

    client = filetransport.FileForwarderClient(datadir=dbd_directory, processor_coro = processor,
                                               force_reread_all=force_reread)
    for s in server:
        if ":" in s:
            try:
                _s, _pub, _req = s.split(":")
            except:
                sys.stderr.write('Server should be of the form localhost or localhost:7000:7001.\n')
                sys.exit(1)
            else:
                try:
                    _pub, _req = int(_pub), int(_req)
                except ValueError:
                    sys.stderr.write('Ports must be specified as integers.\n')
                    sys.exit(2)
        else:
            _s = s
            _pub = pub_port
            _req = req_port
        if _pub == _req:
            sys.stderr.write('PUB and REQ ports must be different.\n')
            sys.exit(3)
        client.add_server(_s, _pub, _req)
    client.print_settings(writer)
    try:
        client.connect_all()
        client.run()
    except Exception as e:
        sys.stderr.write("Runtime Error: %s\n"%(e.args[0]))
        sys.exit(10)
        

