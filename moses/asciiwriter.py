'''
asciiwriter.py

A module to convert glider binary files in to ascii files on a per-sensor
basis, adhering to the naming convention used in the MOSES experiments.

Implemented classes
-------------------

* class MosesDBDWriter(object)


Example
-------
    
m = MosesDBDWriter()

m.set_filename('/home/lucas/gliderdata/nsb3_201907/ld/comet-2019-222-02-000.sbd')

# write all files in one go:
m.write_all()

# or just selected ones
with m:
    m.write("sci_water_temp")
    m.write("salinity")
    m.write("conservative_temperature")

# or process a whole file in one line:
m.process('/home/lucas/gliderdata/nsb3_201907/ld/comet-2019-222-02-000.sbd')
'''
import os.path

import coroutine
import dbdreader
import timeconversion
import fast_gsw

from . import loggers

logger = loggers.get_logger(__name__)

# Functions for derived variables

def salinity_fun(C,T,P, lon, lat):
    return fast_gsw.SA(C*10, T, P*10, lon.mean(), lat.mean())

def potential_density_fun(C,T,P, lon, lat):
    return fast_gsw.pot_rho(C*10, T, P*10, lon.mean(), lat.mean())

def conservative_temperature_fun(C,T,P, lon, lat):
    return fast_gsw.CT(C*10, T, P*10, lon.mean(), lat.mean())

__CTD_PARAMETERS = "sci_water_cond sci_water_temp sci_water_pressure m_gps_lon m_gps_lat".split()

# A database dictionary with:
#      platform translation
#      variable name (sensor in glider-speak) -> measurement device name
#      variable name (sensor in glider-speak) -> human understandable name
#      recipes for computing derived variables, consiting of a tuple with fun, unit and arguments to this function.

DB = dict(platform = dict(amadeus = "GLA",
                          sebastian = "GLB",
                          comet = "GLC",
                          dipsy = "GLD"),
          sensor = dict(sci_water_temp='CTD',
                        sci_water_cond='CTD',
                        sci_water_pressure='CTD',
                        salinity='CTD',
                        conservative_temperature='CTD',
                        potential_density='CTD',
                        sci_flntu_turb_units='FLNTU',
                        sci_flntu_chlor_units='FLNTU',
                        sci_bb3slo_b470_scaled='BB3SLO',
                        sci_bb3slo_b532_scaled='BB3SLO',
                        sci_bb3slo_b660_scaled='BB3SLO',
                        sci_flbbcd_chlor_units='FLBBCD',
                        sci_flbbcd_bb_units='FLBBCD',
                        sci_flbbcd_cdom_units='FLBBCD'),
          variable = dict(sci_water_temp='water_temperature',
                          sci_water_cond='water_conductivity',
                          sci_water_pressure='water_pressure',
                          salinity='salinity',
                          conservative_temperature='conservative_temperature',
                          potential_density='potential_density',
                          sci_flntu_turb_units='turbity',
                          sci_flntu_chlor_units='chlorophylA',
                          sci_bb3slo_b470_scaled='optical_backscatter_470nm',
                          sci_bb3slo_b532_scaled='optical_backscatter_532nm',
                          sci_bb3slo_b660_scaled='optical_backscatter_660nm',
                          sci_flbbcd_chlor_units='chlorophylA',
                          sci_flbbcd_bb_units='optical_backscatter_700nm',
                          sci_flbbcd_cdom_units='colourd_dissolved_organic_matter'),
          derived_variables = dict(salinity=(salinity_fun,'-',__CTD_PARAMETERS),
                                   potential_density=(potential_density_fun, 'kg/m^3', __CTD_PARAMETERS),
                                   conservative_temperature=(conservative_temperature_fun, 'degC', __CTD_PARAMETERS))
          )



class MosesDBDWriter(object):
    ''' Class to write given parameter found in a glider binary data file to 
        a ascii data file, as used in the Moses Project.
    '''
  
    
    def __init__(self, filename=None, comment = "#", output_directory=None):
        ''' Constructor

        Parameters
        ----------

        filename : string
            name of the DBD or SBD file. 
        
        comment : string (default "#")
            character used to a comment characeter.
        
        output_directory : string or None (default None)
            name of directory into which the ascii files are to be written.

        Note
        ----

        The accompanying EBD or TBD file is included automatically.
        '''
        self.data = dict(dbd=None, filename=None, glidername=None, output_directory='.')
        self.coordinate_sensor_names = "m_depth m_gps_lat m_gps_lon".split()
        self.set_filename(filename)
        if output_directory:
            self.set_output_directory(output_directory)
        self.comment_character = comment
        
    def __enter__(self):
        if self.data['filename']:
            self.data['dbd'] = \
                dbdreader.MultiDBD(pattern = self.data['filename'],
                                                       include_paired=True)
        else:
            raise ValueError("No filename specified")
        self.data['glidername'] = self.discover_glidername()
        
    def __exit__(self, ttype, value, traceback):
        if self.data['dbd']:
            self.data['dbd'].close()
        self.data['dbd'] = None

    def discover_glidername(self):
        ''' Discover glider name for currently opened data file 

        Returns
        -------

        glidername : string

        This method provides a way to find out which glider created this data
        file, even if the filename is the internally numeric filename.
        '''
        f = self.data['dbd'].dbds['eng'][0].headerInfo['full_filename']
        glidername, *_ = f.split('-')
        return glidername
    
    def set_filename(self, filename):
        ''' Set the name of the DBD filename

        Parameters
        ----------

        filename : string
            Name of the DBD or SBD filename
        '''
        self.data['filename'] = filename

    def set_output_directory(self, path):
        ''' Set the working directory into which ascii files are to be written 

        Parameters
        ----------

        path : string
            path of directory
        '''
        if not os.path.exists(path):
            raise IOError("Working directory does not exist. (%s)"%(path))
        self.data['output_directory'] = path

        
    def write(self, parameter_name):
        '''Write data file for specified parameter name
        
        Parameters
        ----------

        parameter_name : string
            name of the parameter (sensor) to be written to file.
        
        Note
        ----

        The parameter name must be present in the data file. If not, a
        DBDError will be raised.

        Besides the parameter name, the coordinate parameters will be
        read and they must be present too in the DBD file. These
        parameters are set in self.coordinate_sensor_names and default
        to "m_depth", "m_gps_lat" and "m_gps_lon". The values of these
        parameters are interpolated to the time stamps of the output
        parameter.
        '''

        if parameter_name in DB['derived_variables']:
            fun, unit, args = DB['derived_variables'][parameter_name]
            t, *argsv, d, lat, lon = self.data['dbd'].get_sync(args[0], args[1:] + self.coordinate_sensor_names)
            v = fun(*argsv)
        else:
            t, v, d, lat, lon = self.data['dbd'].get_sync(parameter_name, self.coordinate_sensor_names)
            unit = self.data['dbd'].parameterUnits[parameter_name]
        if t.shape[0] > 0:
            output_filename = self.format_output_filename(parameter_name, t[0])
            self.__write(output_filename, t, d, lat, lon , v, parameter_name, unit)
        # else no data, nothing to write.
        
    def format_output_filename(self, parameter_name, t0):
        ''' Formats the output file name according to the set specifications.

        Parameters
        ----------

        parameter_name : string
            name of the parameter
        t0 : float
            timestamp of first data point in seconds since 1970

        Returns
        -------
        
        s : string
            output filename
  
        Note
        ----

        The time stamp is given as seconds since 1970 and converted to a human readable
        format assuming UTC time zone.
        '''
        glidername = self.data['glidername']
        sensorname = DB['sensor'][parameter_name]
        variable = DB['variable'][parameter_name]
        tstr = timeconversion.epochToDateStr(t0, "%Y%m%dT%H%M%S")
        s = "{glidername}_{sensorname}_{variable}_{tstr}.txt".format(glidername=glidername, sensorname=sensorname, variable=variable, tstr=tstr)
        return s
    
    def __format_header(self, t, parametername, unit):
        ''' Formats the header adapted to the parameter name and its unit.

        Parameters
        ----------

        parametername : string
             name of the parameter to write
        unit : string
             unit of the parameter, as read from the DBD file.

        Returns
        -------

        n : integer
            length of the formatted string of the parameter name and its unit.
        '''
        c = self.comment_character
        glidername = self.data["glidername"]
        platform = DB["platform"][glidername]
        datestr = timeconversion.epochToDateStr(t[0])
        p = "{} ({})".format(parametername, unit)
        header  = "{comment} Platform    : {platform} ({glidername})\n".format(comment=c,
                                                                               platform=platform,
                                                                               glidername=glidername)
        header += "{comment} Parameter   : {parameter}\n".format(comment = c, parameter=parametername)
        header += "{comment} Date        : {datestr}\n".format(comment = c, datestr=datestr)
        header += "{comment} Data source : {filename}\n".format(comment = c, filename=self.data["filename"])
        header += "{comment}\n".format(comment = c)
        header += "{comment} {time:>10s} {depth:>9s} {lat:>15s} {lon:>15s} {parameter:>s}\n".format(comment=c,
                                                                                                    time="time (s)",
                                                                                                    depth="depth (m)",
                                                                                                    lat="latitude (deg)",
                                                                                                    lon="longitude (deg)",
                                                                                                    parameter=p)
        header += "{comment}{marker}\n".format(comment=c, marker="-"*79)
        return len(p), header
    
    def __write(self, fn, t, d, lat, lon, v, parametername, unit):
        ''' Writes the data to file
        
        Parameters
        ----------
        
        fn : string
             name of the output filename
        t, d, lat, lon, v : arrays of floats
            the data to write (time, depth, latitude, longitude and the values)
        parametername : string
            name of the output parameter
        unit : string
            unit of the ouput parameter
        '''
        n, header = self.__format_header(t, parametername, unit)
        _s = "{:12.1f} {:9.1f} {:15.3f} {:15.3f} {:%d.2f}\n"%(n)
        filename = os.path.join(self.data['output_directory'], fn)
        with open(filename, 'w') as fd:
            fd.write(header)
            for _t, _d, _lat, _lon, _v in zip(t, d, lat, lon, v):
                s = _s.format(_t, _d, _lat, _lon, _v)
                fd.write(s)

    def write_all(self):
        ''' Write all sensors

        Tries to write all sensors listen in DB['sensor'] dictionary. 
        Non-existing parameters are silently ignored.

        Returns
        -------
        n : integer
            number of data files written.

        '''
        n = 0
        with self:
            for p in DB['sensor'].keys():
                try:
                    self.write(p)
                except dbdreader.DbdError:
                    pass
                except ValueError as e:
                    if e.args[0] == 'array of sample points is empty':
                        pass # this happens when a file is too short. Then skip it
                    else:
                        raise e
                else:
                    n+=1
        if n==0:
            logger.warning("File (%s) too short, and not processed."%(self.data['filename']))
        return n

    def process(self, filename):
        ''' Process all variables listed in DB['sensor'], as far as the data file
            contains them. Non-existing variables are silently ignored.

        Parameters
        ----------
        filename : string
             name of sbd file. The corresponding tbd file, will be looked up automatically

        Returns
        -------
        n : integer
            number of data files written.
        '''
        self.set_filename(filename)
        return self.write_all()
    



@coroutine.coroutine
def processor(writer, extensions=('sbd', 'tbd')):
    datafiles = dict(sbd=set(), tbd=set())
    k0, k1 = extensions
    while True:
        datadir, filename = (yield)
        basename, ext = os.path.splitext(filename)
        key = ext[1:] # remove the leading dot
        datafiles[key].add(basename)
        available = datafiles[k0].intersection(datafiles[k1])
        if available:
            for f in available:
                fn = os.path.join(datadir, "{}.{}".format(f, k0))
                if writer.process(fn):
                    logger.info("{} Successfully processed.".format(fn))
                datafiles[k0].remove(f)
                datafiles[k1].remove(f)
                
    
