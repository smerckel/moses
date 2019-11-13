import asyncio
import aionotify
from collections import namedtuple
import os
import re

import zmq
import zmq.asyncio

import loggers
logger=loggers.get_logger(__name__)



Connection = namedtuple('Connection', 'server sub req'.split())

class FileTransportServer(object):
    ''' File transport server

    A asynchronous server implementation to
    
    * publish files to any subscriber that may be out there

    * publish the number of files written at regular intervals

    * handle requests made by subscribers:
          * request to return a list of all files transmitted sofar
          * request to return a specific file (from this list)
    '''
    def __init__(self, port, resp_port=None):
        ''' Constructor
        
        Parameters
        ----------
        port : int
            port number for subscribers to connect to
        resp_port : int (default None)
            port number for requests to be made to. If None, this port
            will be one higher than the subscriber port.
        '''
        self.context = zmq.asyncio.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind("tcp://*:%d"%(port))
        resp_port = resp_port or port + 1
        self.responder = self.context.socket(zmq.REP)
        self.responder.bind("tcp://*:%d"%(resp_port))
        self.sent_files = []
        
    async def publish(self,path, filename):
        ''' Coroutine to publish a filename

        Publish the filename as a composite message [b'FILE', binarydata]
        
        Parameters
        ----------
        path : string 
            directory of the file
        filename : string
            name of the file
        '''
        full_path = os.path.join(path, filename)
        with open(full_path, 'rb') as fp:
            bin_data = fp.read()
        mesg = [b"FILE", filename.encode('utf-8'), bin_data]
        await self.publisher.send_multipart(mesg)
        self.sent_files.append((path, filename))
        logger.info("published {}/{}.".format(path, filename))

    async def publish_info(self):
        ''' Coroutine to publish the number of files that have been transmitted '''
        n = "{:d}".format(len(self.sent_files))
        mesg = [b"INFO", n.encode('utf-8')]
        await self.publisher.send_multipart(mesg)
        logger.info("messages transmitted: {}.".format(n))
        
    async def respond_to_requests(self):
        ''' Coroutine to handle requests

        A request can be 
        * LIST: the requestor expects to receive a list of all filenames transmitted
                (so it can figure out which ones it is missing).
        * FILE: the requestor specificially requests a file with the name as argument to FILE
        '''
        bmessage = await self.responder.recv_multipart()
        if bmessage[0] == b'LIST':
            mesg = [b'LIST']
            if self.sent_files:
                mesg += [s.encode('utf-8') for p, s in self.sent_files]
        elif bmessage[0] == b'FILE':
            filename_requested = bmessage[1].decode('utf-8')
            fns = [s[1] for s in self.sent_files]
            try:
                i = fns.index(filename_requested)
            except ValueError:
                mesg = [b'NOFILE']
            else:
                mesg = [b'FILE']
                path = os.path.join(*self.sent_files[i])
                with open(path, 'rb') as fp:
                    mesg.append(fp.read())
        await self.responder.send_multipart(mesg)
        
    def close(self):
        ''' close all sockets '''
        self.publisher.close()
        self.responder.close()
        self.context.term()
        
    
    
class DirWatcher(object):
    ''' Asynchronous directory monitoring class

    Monitors one or more directories, and takes action when files inside this
    directory are closed after writing, that is, a new file is written or a file
    is updated and the process is finished.
    '''
    def __init__(self, loop, regex_pattern="[a-zA-Z]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.[stde]bd"):
        self.loop = loop
        self.watcher = aionotify.Watcher()
        self.regex = re.compile(regex_pattern)

        
    def add_watch(self, path):
        ''' add a directory name to watch

        PARAMETERS
        ----------
        path : string
             path to directory
        '''
        self.watcher.watch(path = path, flags = aionotify.Flags.CLOSE_WRITE)

    def is_valid_filename(self, fn):
        ''' Checks for filename validity
        
        Checks if fn complies with the compiled regular expression self.regex
        Only files that match this pattern will be processed.
        
        PARAMETERS
        ----------
        fn : string
            filename to check
        '''
        r = self.regex.match(fn)
        return r 

    async def __initialise(self):
        # initialise the directory watcher
        await self.watcher.setup(self.loop)

    def initialise(self):
        ''' Initialise the directory watcher.

        Blocking function.
        '''
        self.loop.run_until_complete(self.__initialise())

    async def monitor_directories(self, event_coroutine):
        ''' Monitors directories

        Monitors all watching directories. If files are closed after writing in any of
        these directories AND the filename passes the test is_valid_filename(), then it 
        will be passed to the event_coroutine.

        PARAMETERS
        ----------
        event_coroutine : asyncio coroutine, that accepts (path,filename) as argumnets
            coroutine that will be called and awaited after a valid file has been closed
            after writing.
        '''
        while True:
            event = await self.watcher.get_event()
            if self.is_valid_filename(event.name):
                await event_coroutine(path=event.alias, filename=event.name)

class FileForwarder(object):
    ''' File Forwarder class

    Server implementation that publishes any newly written files in watched directories,
    and handle requests from client to clear up back logs of files that have for some
    reason not been received.
    '''
    
    def __init__(self, ports, *directories, loop=None, info_interval=60):
        ''' constructor

        Parameters
        ----------
        ports : tuple with port numbers (ints)
            portnumbers for pub-sub port and req-rep port
        *directories : strings
            paths of the directories to be monitored.
        loop : event loop (default None)
            event loop. If none, this will lead to asyncio's default eventloop.
        info_interval : int (default 60)
            time interval in seconds between publishing the number of files transmitted.

        Note the pub and rep port numbers should be matched by the client's
        sub and req port numbers.
        '''
        self.loop = loop or asyncio.get_event_loop()
        self.server = FileTransportServer(*ports)
        self.dirwatcher = DirWatcher(self.loop)
        for d in directories:
            self.dirwatcher.add_watch(d)
        self.info_interval = info_interval
        
    async def task_publish_files_sent(self):
        ''' Coroutine to publish the number of files transmitted'''
        while True:
            await self.server.publish_info()
            await asyncio.sleep(self.info_interval)

    async def task_respond_to_requests(self):
        ''' Coroutine to handle requests.'''
        while True:
            await self.server.respond_to_requests()
        
    async def main(self):
        ''' Main coroutine running all tasks concurrently. '''
        tasks = [asyncio.create_task(self.task_publish_files_sent()),
                 asyncio.create_task(self.task_respond_to_requests())]
        await self.dirwatcher.monitor_directories(event_coroutine=self.server.publish)
        self.server.close() # we won't be here though...

    def run(self):
        self.dirwatcher.initialise()
        self.loop.run_until_complete(self.main())



                        

class FileForwarderClient(object):
    ''' Client to the FileForwarder server

    Client implementation to the FileForwarder server(s), subscribing to their publishing channels to
    * receive files to be processed locally
    * number of files transmitted.
    
    If the client figures out there are files missing, it will use a req-rep connection to obtain those 
    directly from the server.
    '''
    
    def __init__(self, datadir='.', processor_coro = None, force_reread_all=False):
        ''' Constructor
        
        PARAMETERS
        ----------
        datadir : string (default '.')
            path to a directory where the resulting files are going to be written.
        
        processor_coro : coroutine (default None)
            coroutine to receive the the name of the file that was written.

        force_reread_all : bool
            All files transmitted by the server will be requested to send again if True.
         
        Notes
        -----
        If the processor_coro is not specified, the file will be written only.

        The setting force_reread_all is True make sense if the client is started much later than the server
        and no files transmitted by the server are locally available, but are required.

        '''
        # Prepare zmq context
        self.context = zmq.asyncio.Context()
        self.connections = []
        self.receptions = []
        self.zmq = dict(FILE=[], INFO=[], REQ=[])
        self.datadir = datadir
        self.processor_coro = processor_coro
        self.force_reread_all = force_reread_all
        
    def add_server(self, server, sub_port=7000, req_port=None ):
        ''' Add a server to monitor.

        PARAMETERS
        ----------
        server : string
            ip adrress or resolvable name of the server
        sub_port : int
            port number of the subscription channel of the server
        req_port : int
            port number of the request-response channel of the server
        '''
        req_port = req_port or sub_port+1
        connection = Connection(server=server, sub=sub_port, req=req_port)
        self.connections.append(connection)
        
    def setup_connections(self):
        ''' Set up all connections for FILE and INFO (pubsub) and REQ (req-rep)'''
        for s, psub, preq in self.connections:
            s_file = self.context.socket(zmq.SUB)
            s_file.setsockopt(zmq.SUBSCRIBE, b"FILE")
            s_info = self.context.socket(zmq.SUB)
            s_info.setsockopt(zmq.SUBSCRIBE, b"INFO")
            s_req = self.context.socket(zmq.REQ)
            self.zmq['FILE'].append(s_file)
            self.zmq['INFO'].append(s_info)
            self.zmq['REQ'].append(s_req)
            self.receptions.append([]) # list of received files.

    def connect(self):
        ''' Make all connections'''
        for i, (s, psub, preq) in enumerate(self.connections):
            self.zmq['FILE'][i].connect("tcp://%s:%d"%(s, psub))
            self.zmq['INFO'][i].connect("tcp://%s:%d"%(s, psub))
            self.zmq['REQ'][i].connect("tcp://%s:%d"%(s, preq))

    def close(self):
        ''' Close all connections '''
        for k, v in self.zmq.items():
            for s in v:
                s.close()

    def write_file(self, filename, contents):
        ''' Write a file

        PARAMETERS
        ----------
        filename : string
            name of the file to write
        contents : binary string
            contents to be written (without formatting)

        NOTE
        ----
        The processing of a file depends on whether or not its accompanying file is present.
        For now, files are written to the data directory, and processed when both pairs are 
        written.
        '''
        logger.info("Writing file {}".format(filename))
        path = os.path.join(self.datadir, filename)
        with open(path, 'wb') as fp:
            fp.write(contents)
        if not self.processor_coro is None:
            self.processor_coro.send((self.datadir, filename))
        
    async def listen(self, i):
        ''' Coroutine to listen for incoming files

        This coroutine waits for a data package to arrive and writes it on reception.

        PARAMETERS
        ----------
        i : int
            index number of the server.
        '''
        connection = self.zmq['FILE'][i]
        try:
            [address, filename, contents] = await connection.recv_multipart()
        except zmq.ZMQError as e:
            sys.stderr("Error reading zmq message (error=%d)\n"%(e.errno))
            contents = b""
            filename = b""

        filename = filename.decode('utf-8').lower()
        if filename:
            self.receptions[i].append(filename)
            self.write_file(filename, contents)

    async def listen_info(self, i):
        ''' Coroutine to listen for incoming files

        This coroutine waits for a data package to arrive which has a payload
        the number of files have been transferred already by the sending end.

        PARAMETERS
        ----------
        i : int
            index number of the server.
        '''
        server = self.connections[i]
        connection = self.zmq['INFO'][i]
        try:
            [address, ns] = await connection.recv_multipart()
        except zmq.ZMQError as e:
            sys.stderr("Error reading zmq message (error=%d)\n"%(e.errno))
            n = None
        else:
            n = int(ns.decode('utf-8'))
        return n
    
    async def make_request(self, i, request, *p):
        ''' Coroutine to make a general request to the server '''
        logger.info("Making request ({}) to server {}".format(i))
        mesg = [ request.encode('utf-8') ]
        mesg += [_p.encode('utf-8') for _p in p]
        await self.zmq['REQ'][i].send_multipart(mesg)
        response = await self.zmq['REQ'][i].recv_multipart()
        return response
    
    async def initialise(self):
        ''' Coroutine to initialise the connection

        The coroutine polls all configured servers and expect to receive a list
        of files that each server has transmitted sofar. The routine will return
        ONLY if ALL servers have responded.
        '''
        # check if we have a directory where we can write the data files.
        if not os.path.exists(self.datadir):
            raise IOError('Data directory (%s) not existing.'%(self.datadir))
        logger.info("Waiting for first contact... I will wait until all servers are reached.")
        n = len(self.connections)
        logger.info("Polling:")
        for c in self.connections:
            logger.info("\t{}:{}/{}".format(c.server, c.sub, c.req))
        tasks = [asyncio.create_task(self.make_request(i,'LIST')) for i in range(n)]
        results = await asyncio.gather( *tasks )
        if not self.force_reread_all:
            # assume that what the server has sent, we already have
            # and fill self.receptions accordingly.
            for i, r in enumerate(results):
                r.pop(0) # remove the "command"
                self.receptions[i] = [_r.decode('utf-8').lower() for _r in r]
        logger.info("Initialised.")

    async def clear_backlog(self, i):
        ''' Clear backlog
    
        Clears any existing backlog the client has with server i.
        
        PARAMETERS
        ----------
        i : int
            index number of server.
        '''
        response = await self.make_request(i,'LIST')
        available_files = set([_r.decode('utf-8').lower() for _r in response[1:]])
        received_files = set(self.receptions[i])
        for f in available_files.difference(received_files):
            response = await self.make_request(i, 'FILE', f)
            self.receptions[i].append(f) # successfully read file f
            content = response[1]
            self.write_file(f, content)
            
    async def main(self):
        '''Coroutine main

        Main routine to service all connections with all servers.
        
               
        The routine sets up all sub connections for files and number
        of files transmitted publishers and runs them in the
        background.  When one of the subscriptions is completed, it
        will be respanwed.  If a data packet is received with the
        number of files transmitted, and this is not the same as our
        logs here, we have to fetch some files from the server. For
        this a separate task is spawned if none is already running for
        this.
        '''
        n = len(self.connections)
        tasks = dict(FILE=[asyncio.create_task(self.listen(i)) for i in range(n)],
                     INFO=[asyncio.create_task(self.listen_info(i)) for i in range(n)],
                     REQ=[])
        backlog_tasks = dict()
        
        while True:
            await asyncio.sleep(0.5)
            # for each task that received a file (and is finished), spawn it again.
            for i, t in enumerate(tasks['FILE']):
                if t.done():
                    tasks['FILE'][i] = asyncio.create_task(self.listen(i))
            # for each task that processed in file number count and is finished, spawn it again.
            # also span to remove the back log, if necessary.
            for i, t in enumerate(tasks['INFO']):
                if t.done():
                    tasks['INFO'][i] = asyncio.create_task(self.listen_info(i))
                    if self.receptions[i]:
                        files_received = len(self.receptions[i])
                    else:
                        files_received = 0
                    files_sent = t.result()
                    if files_received != files_sent:
                        # we have a back log of files. Start a new tasks if for this server is non running already.
                        if not i in backlog_tasks:
                            logger.info("Starting backlog clearance process...")
                            backlog_tasks[i] = asyncio.create_task(self.clear_backlog(i))
            # remove any finished backlog_tasks...
            removables=[]
            for i, t in backlog_tasks.items():
                if t.done():
                    removables.append(i)
            for i in removables:
                backlog_tasks.pop(i)
                logger.info("Finished backlog clearance process.")
    def run(self, loop = None):
        loop = loop or asyncio.get_event_loop()
        loop.run_until_complete(self.initialise())
        loop.run_until_complete(self.main())
