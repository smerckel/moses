from ftplib import FTP
from getpass import getpass
from hashlib import sha256 
from itertools import chain
import os
import sys

import numpy as np
from scipy.interpolate import interp1d

import dbdreader

from . import coriolis_data
from . import loggers

logger = loggers.get_logger(__name__)

PASSWORDLENGTH = 17
def hash_password(s):
    b = s.encode('utf-8')
    return sha256(b).hexdigest()[:PASSWORDLENGTH]
    
class CoriolisDataFormat(object):

    def __init__(self, parameter_list, output_dir):
        ''' Constructor
        
        Parameters
        ----------
        parameter_list : list of strings or None
            list of glider sensors, as strings. If None, all parameters are 
            selected.
        output_dir : string
            name of the directory to write the converted files into.

        Attributes
        ----------
        parameters : list of string
            list of glider sensors
        output_dir : string
            name of the directory to write the converted files into.

        '''
        self.parameters = parameter_list
        self.output_dir = output_dir
        try: # make sure output dir exists.
            os.makedirs(self.output_dir)
        except OSError:
            pass # alread exists.
            
    def _set_parameters(self, dbd):
        if self.parameters is None:
            self.parameters = [i for i in chain(*dbd.parameterNames.values())]
    
    def convert(self, filename, force=False):
        ''' Convert a dbd file (and its accompanying ebd file into
        a matlab .m and and an ascii file.

        Parameters
        ----------
        filename : string
            a filename pointing to a Slocum DBD file.

        force: bool
            If True it will overwrite any existing files. If False,
            no files will be overwritten and the return value is None.

        Returns
        -------
        tuple of .m filename and .dat filename (full path).
        or
        None if files already exist and force==False

        '''
        banned_missions = "autoexec.mi status.mi initial.mi lastgasp.mi overtime.mi overdpth.mi trim.mi ini0.mi ini1.mi".split()
        try:
            dbds = dbdreader.MultiDBD(filenames=[filename], complement_files=True,
                                      banned_missions = banned_missions)
        except dbdreader.DbdError as e:
            code = e.args[0]
            if code == dbdreader.DBD_ERROR_ALL_FILES_BANNED:
                logger.info("All submitted files were considered banned missions. Nothing processed.")
                return None
            else:
                # unhandled error. Reraise error.
                raise e
        self._set_parameters(dbds)
        try:
            data = dict([(p,dbds.get(p, decimalLatLon=False))
                         for p in self.parameters])
        except dbdreader.DbdError:
            logger.error("In convert(): failed to read all requested parameters from {stde}bd files!")
            return None

        run_name, matlab_fn  = self.get_fn_names(dbds)
        start_time = self.get_start_time(data)
        output_filename_m = os.path.join(self.output_dir, matlab_fn)
        output_filename_dat = output_filename_m.replace(".m", ".dat")

        # Don't do any writing if the files already exist and we done't want to force writing them.
        if os.path.exists(output_filename_dat) and os.path.exists(output_filename_m) and not force:
            return None

        # Check whether we have at least 2 data points. For less, write_data_ascii breaks on the interpolation
        matfile_to_be_written = False
        for p, (t,v) in data.items():
            if len(t)>=2:
                matfile_to_be_written=True
                break
        if not matfile_to_be_written:
            logger.info(f"File {output_filename_dat} not written, because of lack of data. Not writing {output_filename_m} either.")
            return  None
        
        logger.debug(f"Writing data for {filename}")
        with open(output_filename_m,'w') as fp:
            self.write_header(fp, run_name, start_time)
            self.write_data(fp, data)
            self.write_footer(fp, matlab_fn)
            
        with open(output_filename_dat, 'w') as fp:
            self.write_data_ascii(fp, data)

        logger.debug(f"Files written for {filename}")
        return output_filename_m, output_filename_dat
    
    def get_start_time(self, data):
        ''' Return start time of data file

        Parameters
        ----------
        data : dictionary with parameter names (keys) and tuples with time and values

        Returns
        -------
        timestamp (s)
        '''
        t0 = [t[0] for t,v in data.values() if len(t)]
        return min(t0)
    
    def get_fn_names(self, dbds):
        ''' Return filenames as specified in the dbd headers

        Parameters
        ----------
        dbds : an instance of MultiDBD

        Returns
        -------
        run_name : string of running name (composed of full name and 8x3 name)
        matlab_filename : the full_filename with - replaced by _, and -dbd.m extension
        '''

        try:
            header_info = dbds.dbds['eng'][0].headerInfo
        except IndexError:
            header_info = dbds.dbds['sci'][0].headerInfo
        if dbds.filenames[0].endswith('sbd') or dbds.filenames[1].endswith('sbd'):
            extension='sbd'
        else:
            extension='dbd'

        run_name="{}-{}({})".format(header_info['full_filename'],
                                    extension,
                                    header_info['the8x3_filename'])
        matlab_filename = "{}-{}.m".format(header_info['full_filename'],extension).replace("-","_")
        return run_name, matlab_filename

    def write_header(self, fp, run_name, start_time):
        ''' Writes header of the .m part of the data file pairs
        
        Parameters
        ----------
        fp : file pointer
        run_name : run_name variable
        start_time : start time of the data file (in seconds since 1970)
        '''
        fp.write("global run_name\n")
        fp.write("global data\n")
        fp.write("run_name = '{}';\n".format(run_name))
        fp.write("clear time0\n")
        fp.write("start = {:f};\n".format(start_time))

    def write_data(self, fp, data):
        ''' Writes the body of the .m part of the data file pairs
        
        Parameters
        ----------
        fp : file pointer
        data : dictionary with the dbd data read.
        '''

        k = 'm_present_time'
        fp.write("global {}\n".format(k))
        fp.write("{} = {:d};\n".format(k,1))
        for i, (k, (t,v)) in enumerate(data.items()):
            fp.write("global {}\n".format(k))
            fp.write("{} = {:d};\n".format(k,i+2)) # + 2 as we have m_present_time first, and matlab offsets at 1
        
    def write_footer(self, fp, matlab_filename):
        ''' Writes footer of the .m part of the data file pairs
        
        Parameters
        ----------
        fp : file pointer
        matlab_filename : the name of the .m file.
        '''
        mat_fn = matlab_filename.replace(".m", ".dat")
        base_fn = matlab_filename.replace(".m", "")
        fp.write("load('{}')\n".format(mat_fn))
        fp.write("data = {};\n".format(base_fn))
        fp.write("clear {}\n".format(base_fn))

    def write_data_ascii(self, fp, data):
        ''' Writes the accompanying ascii data file
        
        Parameters
        ----------
        fp : file pointer
        data : dictionary with the dbd data read.
        '''

        # get all timestamps available, remove dupes and sort them...
        all_timestamps = np.array([t for t in set(np.hstack([t for k, (t,v) in data.items()]))])
        all_timestamps.sort()
        # define an interpolation function to find the appropriate index for each time stamp:
        ifun = interp1d(all_timestamps, np.arange(all_timestamps.shape[0]))
        # fill the data
        keys0 = list(data.keys())
        for k,(t,v) in data.items():
            expanded_data = np.zeros_like(all_timestamps)*np.nan
            idx = ifun(t).astype(int)
            expanded_data[idx] = v
            data[k] = expanded_data
        keys1 = list(data.keys())
        if (keys0!=keys1):
            raise ValueError("Order of keys changed. cannot happen!")
        fmt = " ".join(["%.8f"]*(len(data.keys())+1))
        for line in zip(all_timestamps, *data.values()):
            s=(fmt%(line))
            s=s.replace("nan", "NaN")
            fp.write("%s\n"%(s))
    
class Coriolis_FTP_Transfer(object):

    def __init__(self, target, ID, working_directory, skip_ftp_transfer=False):
        '''Class for managing conversion of glider binaries and transfer to Coriolis

        Parameters
        ----------
        target : str
            ftp target: 'coriolis' or 'debug'
        ID : str
            ID of the experiment on the Coriolis ftp host, usually a date such as 20191123 
        working_directory : str
            path of directory where converted files are written into.
        skip_ftp_transfer : bool
            skips the ftp transfer if True (for debugging and testing only)
        '''
        self.server_info = coriolis_data.CREDENTIALS[target]
        self.ID = ID
        self.working_directory = working_directory
        self.skip_ftp_transfer = skip_ftp_transfer
        self.ask_for_password_if_needed()
        
    def ask_for_password_if_needed(self):
        if self.server_info.password is None:
            p = f"Password for {self.server_info.host}:"
            p_hashed = hash_password(getpass(p))
            self.server_info = coriolis_data.FTP_Credentials(self.server_info.host,
                                                             self.server_info.user,
                                                             p_hashed,
                                                             self.server_info.rootdir)
            
    def get_configuration_repr(self):
        ''' Return a string specifying the configuration of this class 

        Returns
        -------
        str
            description of current configuration
        '''
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
        logger.debug(f"Going to transfer {len(lof)} file via FTP...")
        for f in lof:
            _, fn = os.path.split(f)
            with open(f, 'rb') as fp:
                ftp.storlines("STOR {}".format(fn), fp)
                logger.debug(f"Transferred {fn}.")
                
    def transfer_files(self, glider, lof):
        ''' transfer files via ftp
        
        Parameters
        ----------
        glider : str
            glider name
        lof : list of str
            list of files

        Given the current configuration, a glider dependent destination is created,
        logged in on the the ftp server, and the files, indicated by the lof are
        transferred.
        '''
        path = self._generate_server_path(glider)
        logger.debug(f"Path at remote server: {path}")
        _s = self.server_info
        with FTP(_s.host, _s.user, _s.password) as ftp:
            ftp.login(_s.user, _s.password)
            logger.debug("Logged in")
            ftp.cwd(path)
            logger.debug(f"Cdir to {path}.")
            self._write_files(ftp, lof)

    def get_glidername(self, dbd_filename):
        ''' Get glidername for dbd binary file
        
        Parameters
        ----------
        dbd_filename : str
            name of glider binary file
        '''
        path, fn = os.path.split(dbd_filename)
        glider, *_ = fn.split('-')
        return glider
    
    def process(self, dbd_filename):
        ''' convert and download this file and its companion

        Parameters
        ----------
        dbd_filename : str
            name of glider binary file

        Returns
        -------
        bool
            True if succesful, False if lof is empty or skip_ftp_transfer==True

        The glider binary filename, either a sbd or dbd file, is converted into
        a pair of matlab files. The name of the glider is inferred from the 
        binary filename, and the parameters to write into the matlab files is governed by
        the settings in coriolis_data.PARAMETERS for this particular glider.

        Notes
        -----
        
        The current implementation does not allow to prescribe a list of parameters. My current view
        is that the parameter list as given in coriolis_data.PARAMETERS dictionary, should be as 
        extensive as possible. Parameters that specified in this dictionary, but are not present in the 
        binary data files are ignored (but a warning is issued).
        '''
        glider = self.get_glidername(dbd_filename)
        parameter_list = coriolis_data.PARAMETERS[glider]
        converter = CoriolisDataFormat(parameter_list, output_dir = self.working_directory)
        lof = converter.convert(dbd_filename)
        if not lof is None and not self.skip_ftp_transfer:
            self.transfer_files(glider, lof)
            return True
        else:
            return False
    
