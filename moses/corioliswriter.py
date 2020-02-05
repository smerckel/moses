from ftplib import FTP
import os
import sys

from dbdreader import DbdError

import coriolis.coriolis as coriolis
from . import coriolis_data

class Coriolis_FTP_Transfer(object):

    def __init__(self, target, ID, working_directory, skip_ftp_transfer=False):
        self.server_info = coriolis_data.CREDENTIALS[target]
        self.ID = ID
        self.working_directory = working_directory
        self.skip_ftp_transfer = skip_ftp_transfer

    def get_configuration_repr(self):
        ''' Return a string specifying the configuration of this class '''
        m = ['Coriolis_FTP_Transfer:']
        m.append('server: %s'%self.server_info.__repr__())
        m.append('ID : %s'%(self.ID))
        m.append('working_directory : %s'%(self.working_directory))
        m.append('skip_ftp_transfer :%d'%(self.skip_ftp_transfer))
        m.append('--')
        return m
        
    def _generate_server_path(self, glider):
        path = "/".join([self.server_info.rootdir, glider,
                         "{}_{}".format(glider, self.ID)])
        return path

    def _write_files(self, ftp, lof):
        for f in lof:
            _, fn = os.path.split(f)
            with open(f, 'rb') as fp:
                ftp.storlines("STOR {}".format(fn), fp)
                          
    def transfer_files(self, glider, lof):
        path = self._generate_server_path(glider)
        _s = self.server_info
        with FTP(_s.host, _s.user, _s.password) as ftp:
            ftp.login(_s.user, _s.password)
            ftp.cwd(path)
            self._write_files(ftp, lof)

    def get_glidername(self, dbd_filename):
        path, fn = os.path.split(dbd_filename)
        glider, *_ = fn.split('-')
        return glider
    
    def process(self, dbd_filename):
        ''' convert and download this file and its companion '''
        glider = self.get_glidername(dbd_filename)
        parameter_list = coriolis_data.PARAMETERS[glider]
        converter = coriolis.CoriolisDataFormat(parameter_list, output_dir = self.working_directory)
        lof = converter.convert(dbd_filename)
        if not lof is None and not self.skip_ftp_transfer:
            self.transfer_files(glider, lof)
            return True
        else:
            return False
    
