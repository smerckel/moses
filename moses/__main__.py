import argparse
import sys

from dbdreader import DbdError

from . import filetransport
from . import asciiwriter
from . import corioliswriter
from . import loggers

logger = loggers.get_logger(__name__)

def gen_error(errorno, errormsg):
    sys.stderr.write("\n")
    sys.stderr.write("Runtime error: {errormsg}\n".format(errormsg=errormsg))
    sys.stderr.write("\n")
    sys.exit(errorno)


def script_moses_dbd_server():
    description='''
Glider DBD file Publisher

A server program that publishes DBD files as soon as they are written by the dockserver.
At regular intervals, the program submits the number of files transferred, so that a client
can figure out if there are any missing and fetch those on request.

'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--pub_port', type=int, help='Port number used for publishing data (default: 7000)', default=7000)
    parser.add_argument('--req_port', type=int, help='Port number used for accepting queries from clients (default: 7001)', default=7001)
    parser.add_argument('--interval', type=int, help='Publishing interval of number files sent in seconds', default=600)
    parser.add_argument('directory', help='Directory to monitor (supply as many as needed, and may include log directories)', nargs='+')
    args = parser.parse_args()

    pub_port = args.pub_port
    req_port = args.req_port
    info_interval = args.interval
    directory = args.directory
    #regex_pattern=".*\.log|.*\.[st]bd"
    regex_pattern = "[a-zA-Z].*\.log|[a-zA-Z][a-zA-Z0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.[stde]bd"
    FW = filetransport.FileForwarder((pub_port, req_port), *directory, info_interval=info_interval, regex_pattern=regex_pattern)
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
    parser.add_argument('--pub_port', type=int, help='(Common) port number used for publishing data (default: 7000)', default=7000)
    parser.add_argument('--req_port', type=int, help='(Common) port number used for accepting queries from clients (default: 7001)', default=7001)

    parser.add_argument('--server', '-s', help='Publishing server(s) to connect to. Format is server (using common port numbers, or server:pub_port:req_port to use specific port numbers.', action='append')
    parser.add_argument('--force-reread', action='store_true', help='Force rereading all dbd files when the client is started. If not specified, the client will not attempt to retrieve any files that are reported missing at start up.')
    parser.add_argument('--processor', help=''''Post-process data. Options are 
  * "ascii", which converts the dbd files into ascii files
  * "coriolis", which pushes the data to the coriolis ftp site.''', choices=['ascii','coriolis'], action='append')
    parser.add_argument('--ascii_processor_directory', help='Directory for the ascii processor to write the converted data files to')
    parser.add_argument('--coriolis_processor_directory', help='Directory where converted .m and .dat files are written to')
    parser.add_argument('--coriolis_skip_ftp_transfer', action='store_true', help='If set, the actual ftp transfer is is not executed')
    parser.add_argument('--coriolis_target', default='coriolis', help='Sets the FTP target, useful for testing (default: coriolis).')
    parser.add_argument('--coriolis_id', help='ID for subdirectory on coriolis server (start date of experiment. Example: 20191123')
                        
    args = parser.parse_args()
    server = args.server
    dbd_directory = args.dbd_directory
    processor = args.processor
    force_reread = args.force_reread
    ascii_directory = args.ascii_processor_directory
    pub_port = args.pub_port
    req_port = args.req_port
    coriolis_processor_directory = args.coriolis_processor_directory
    coriolis_skip_ftp_transfer = args.coriolis_skip_ftp_transfer
    coriolis_target = args.coriolis_target
    coriolis_id = args.coriolis_id

    writers = []
    if processor:
        for p in processor:
            if p == 'ascii':
                writers.append(asciiwriter.MosesDBDWriter(output_directory=ascii_directory))
            elif p == 'coriolis':
                # check all required (non-default parameters)
                if coriolis_id is None:
                    sys.stderr.write('Coriolis ID MUST be specified.\n')
                    sys.exit(2)
                if coriolis_processor_directory is None:
                    sys.stderr.write('Coriolis processor directory MUST be specified.\n')
                    sys.exit(3)
                _w = corioliswriter.Coriolis_FTP_Transfer(target = coriolis_target,
                                                          ID = coriolis_id,
                                                          working_directory = coriolis_processor_directory,
                                                          skip_ftp_transfer = coriolis_skip_ftp_transfer)
                writers.append(_w)
            else:
                raise NotImplementedError("Processor type not implemented.")
        processor_coro = asciiwriter.processor(writers, extensions=('sbd', 'tbd'))
        # To test with simulator (sbd files only, use this processor:
        #processor_coro = asciiwriter.processor(writers, extensions=('sbd', None))
    else:
        processor_coro = None
    logger.debug(f"writers: {writers}")
    client = filetransport.FileForwarderClient(datadir=dbd_directory, processor_coro = processor_coro,
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
    for writer in writers:
        client.print_settings(writer)
    try:
        client.connect_all()
        client.run()
    except Exception as e:
        sys.stderr.write("Runtime Error: %s\n"%(e.args[0]))
        sys.exit(10)
        


def script_coriolis_upload():
    description='''
Bulk upload files to Coriolis using FTP transfer.

A program that allows to create and transfer datafiles for submission to the Coriolis Data Center.
The key argument to supply is the filenames to be processed. This can be either sbd or dbd files. 
The corresponding tbd or ebd files are assumed to be in the same directory.
'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('filenames', nargs='+', help='List of sbd or dbd files to be processed and transported.')
    parser.add_argument('--processor_directory', help='Directory where converted .m and .dat files are written to')
    parser.add_argument('--skip_ftp_transfer', action='store_true', help='If set, the actual ftp transfer is is not executed')
    parser.add_argument('--target', default='coriolis', help='Sets the FTP target, useful for testing.')
    parser.add_argument('--id', help='ID for subdirectory on coriolis server (start date of experiment. Example: 20191123')
                        
    args = parser.parse_args()
    filenames = args.filenames
    coriolis_processor_directory = args.processor_directory
    coriolis_skip_ftp_transfer = args.skip_ftp_transfer
    coriolis_target = args.target
    coriolis_id = args.id

    endings = [i.endswith('sbd') or i.endswith('dbd') for i in filenames]
    if not all(endings):
        gen_error(1, 'Not files seem to have the sbd or dbd extension...')
    if coriolis_processor_directory is None:
        gen_error(2, "Specify working_directory for the data processer (--coriolis_processor_directory)")
    if coriolis_id is None:
        gen_error(4,"Specify ID to use at the coriolis FTP site, usually start of experiment. (--coriolis_id)")
    
    c_ftp_transfer = corioliswriter.Coriolis_FTP_Transfer(target=coriolis_target,
                                                          ID=coriolis_id,
                                                          working_directory=coriolis_processor_directory,
                                                          skip_ftp_transfer=coriolis_skip_ftp_transfer)
    for f in filenames:
        print("Processing {}".format(f))
        try:
            processed = c_ftp_transfer.process(f)
            if not processed:
                print("Files already present (or transfer skipped).")
        except DbdError as e:
            print(e)




def script_convert_for_coriolis():
    description='''
Bulk conversion of glider files into .m/.dat pairs.

Converts dbd/ebd or sbd/tbd pairs into m/dat file pairs, which can be processed further by processing 
tools by Coriolis.
'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('filenames', nargs='+', help='List of sbd or dbd files to be processed and transported.')
    parser.add_argument('--processor_directory', help='Directory where converted .m and .dat files are written to')
                        
    args = parser.parse_args()
    filenames = args.filenames
    coriolis_processor_directory = args.processor_directory

    endings = [i.endswith('sbd') or i.endswith('dbd') for i in filenames]
    if not all(endings):
        gen_error(1, 'Not files seem to have the sbd or dbd extension...')
    if coriolis_processor_directory is None:
        gen_error(2, "Specify working_directory for the data processer (--coriolis_processor_directory)")
    
    c_processor = corioliswriter.Coriolis_FTP_Transfer(target='debug',
                                                       ID=None,
                                                       working_directory=coriolis_processor_directory,
                                                       skip_ftp_transfer=True)
    for f in filenames:
        print("Processing {}".format(f))
        try:
            processed = c_processor.process(f)
        except DbdError as e:
            print(e)

            
def script_hash_password():
    transfer = corioliswriter.Coriolis_FTP_Transfer('coriolis', '2091123', 'tmp')
    n = corioliswriter.PASSWORDLENGTH
    print("Hashed password:")
    print(transfer.server_info.password[:n])
    
