#!/usr/bin/env python

"""
@package mi.dataset.parser.zplsc_c_dlc
@file marine-integrations/mi/dataset/parser/zplsc_c_dcl.py
@author Richard Han
@brief Parser for the zplsc_c_dcl dataset driver

Release notes:

Initial Release
"""

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'


import calendar
import ntplib
import re
import os
import numpy as np
from multiprocessing import Process
from datetime import datetime, timedelta
from struct import unpack
from collections import defaultdict

from mi.dataset.dataset_parser import SimpleParser
from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException
from mi.core.instrument.data_particle import DataParticle
from mi.core.log import get_logging_metaclass

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.zplsc_echogram import generate_plots


from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    TIMESTAMP, START_METADATA, END_METADATA, START_GROUP, END_GROUP, SENSOR_GROUP_TIMESTAMP

from mi.dataset.parser.common_regexes import FLOAT_REGEX, UNSIGNED_INT_REGEX, INT_REGEX, SPACE_REGEX, ANY_CHARS_REGEX, \
    ASCII_HEX_CHAR_REGEX, ONE_OR_MORE_WHITESPACE_REGEX

# Basic patterns
UINT = '(' + UNSIGNED_INT_REGEX + ')'   # unsigned integer as a group
SINT = '(' + INT_REGEX + ')'            # signed integer as a group
FLOAT = '(' + FLOAT_REGEX + ')'         # floating point as a captured group
MULTI_SPACE = SPACE_REGEX + '+'
ANY_NON_BRACKET_CHAR = r'[^\[\]]+'

# DCL Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
SENSOR_DATE = r'\d{4}/\d{2}/\d{2}'          # Sensor Date: YYYY/MM/DD
SENSOR_TIME = r'\d{2}:\d{2}:\d{2}.\d{2}'    # Sensor Time: HH:MM:SS.mm
TWO_HEX = '(' + ASCII_HEX_CHAR_REGEX + ASCII_HEX_CHAR_REGEX + ')'

# Serial number in the instrument deployment file
QUOTE = r'\''
SERIAL_NUMBER_PATTERN = r'Unit Serial Number'
SERIAL_NUMBER_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
SERIAL_NUMBER_PATTERN += UINT
SERIAL_NUMBER_MATCHER = re.compile(SERIAL_NUMBER_PATTERN)

# Channel 1 frequency in the instrument deployment file
CHAN1_FREQUENCY_PATTERN = r'F1 Frequency'
CHAN1_FREQUENCY_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CHAN1_FREQUENCY_PATTERN += UINT
CHAN1_FREQUENCY_MATCHER = re.compile(CHAN1_FREQUENCY_PATTERN)

# Channel 2 frequency in the instrument deployment file
CHAN2_FREQUENCY_PATTERN = r'F2 Frequency'
CHAN2_FREQUENCY_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CHAN2_FREQUENCY_PATTERN += UINT
CHAN2_FREQUENCY_MATCHER = re.compile(CHAN2_FREQUENCY_PATTERN)

# Channel 3 frequency in the instrument deployment file
CHAN3_FREQUENCY_PATTERN = r'F3 Frequency'
CHAN3_FREQUENCY_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CHAN3_FREQUENCY_PATTERN += UINT
CHAN3_FREQUENCY_MATCHER = re.compile(CHAN3_FREQUENCY_PATTERN)

# Channel 4 frequency in the instrument deployment file
CHAN4_FREQUENCY_PATTERN = r'F4 Frequency'
CHAN4_FREQUENCY_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
CHAN4_FREQUENCY_PATTERN += UINT
CHAN4_FREQUENCY_MATCHER = re.compile(CHAN4_FREQUENCY_PATTERN)

# Sound Speed pattern in the instrument deployment file
SOUND_SPEED_PATTERN = r'SoundSpeed'
SOUND_SPEED_PATTERN += ONE_OR_MORE_WHITESPACE_REGEX
SOUND_SPEED_PATTERN += FLOAT
SOUND_SPEED_MATCHER = re.compile(SOUND_SPEED_PATTERN)

# DCL Log record:
# Timestamp [Text]MoreText newline
DCL_LOG_PATTERN = TIMESTAMP + SPACE_REGEX   # DCL controller timestamp
DCL_LOG_PATTERN += START_METADATA           # Metadata record starts with '['
DCL_LOG_PATTERN += ANY_NON_BRACKET_CHAR     # followed by text
DCL_LOG_PATTERN += END_METADATA             # followed by ']'
DCL_LOG_PATTERN += ANY_CHARS_REGEX          # followed by more text
DCL_LOG_MATCHER = re.compile(DCL_LOG_PATTERN)

# DCL Power On Log record:
# 2013/12/01 01:00:20.797 [adcp:DLOGP1]:Instrument Started [Power On]\n
DCL_POWER_ON_PATTERN = TIMESTAMP + SPACE_REGEX                # DCL controller timestamp
DCL_POWER_ON_PATTERN += START_METADATA                        # Metadata record starts with '['
DCL_POWER_ON_PATTERN += ANY_NON_BRACKET_CHAR                  # followed by text
DCL_POWER_ON_PATTERN += END_METADATA                          # followed by ']'
DCL_POWER_ON_PATTERN += '.*? \[Power On\]'      # followed by power on text
DCL_POWER_ON_MATCHER = re.compile(DCL_POWER_ON_PATTERN)

# DCL Power Off Log record:
# 2013/12/01 01:06:08.038 [adcp:DLOGP1]:Instrument Stopped [Power Off]\n
DCL_POWER_OFF_PATTERN = TIMESTAMP + SPACE_REGEX               # DCL controller timestamp
DCL_POWER_OFF_PATTERN += START_METADATA                       # Metadata record starts with '['
DCL_POWER_OFF_PATTERN += ANY_NON_BRACKET_CHAR                 # followed by text
DCL_POWER_OFF_PATTERN += END_METADATA                         # followed by ']'
DCL_POWER_OFF_PATTERN += '.*?Power Off'  # followed by power off text
DCL_POWER_OFF_MATCHER = re.compile(DCL_POWER_OFF_PATTERN)

# Header 1
# 2013/12/01 01:04:18.213 2013/12/01 01:00:27.53 00001\n
SENSOR_TIME_PATTERN = TIMESTAMP + MULTI_SPACE  # DCL controller timestamp
SENSOR_TIME_PATTERN += START_GROUP + SENSOR_DATE + MULTI_SPACE  # sensor date
SENSOR_TIME_PATTERN += SENSOR_TIME + END_GROUP + MULTI_SPACE    # sensor time
SENSOR_TIME_PATTERN += UINT                                     # Ensemble Number
SENSOR_TIME_MATCHER = re.compile(SENSOR_TIME_PATTERN)

# zplsc profile data starts with 0xfd02 hex flag
SAMPLE_REGEX = r'\xfd\x02'
SAMPLE_MATCHER = re.compile(SAMPLE_REGEX, re.DOTALL)

BLOCK_SIZE = 1024*4             # Block size read in from binary file to search for token

CHANNEL_1 = 1
CHANNEL_2 = 2
CHANNEL_3 = 3
CHANNEL_4 = 4
MAX_CHANNEL = 4
KHZ = 1000

# Set up resource path Temporary for testing
RESOURCE_PATH = os.path.join('/Users/rhan/mi-dataset', 'mi', 'dataset', 'driver',
                             'zplsc_a', 'dcl', 'resource')
DPL_FILE_NAME = '14102414.DPL'

# Numpy data type object for unpacking the Sample datagram from the binary file
sample_dtype = np.dtype([('cf_flag', 'a2'),                 # 2 bytes Compact Flag)
                      # Profile Data
                      ('burst_number', 'i2'),               # Profile number
                      ('serial_number', 'u2'),              # The instrument serial number
                      ('ping_status', 'i2'),                # Status
                      ('burst_interval', 'i4'),             # burst interval in Seconds
                      ('year', 'i2'),                       # Year
                      ('month', 'i2'),                      # Month
                      ('day', 'i2'),                        # Day
                      ('hour', 'i2'),                       # Hour
                      ('minute', 'i2'),                     # Minute
                      ('second', 'i2'),                     # Seconds
                      ('hundreds', 'i2'),                   # Hundreds of seconds
                      ('digit_rate_chan1', 'i2'),           # Channel 1 digitization rate
                      ('digit_rate_chan2', 'i2'),           # Channel 2 digitization rate
                      ('digit_rate_chan3', 'i2'),           # Channel 3 digitization rate
                      ('digit_rate_chan4', 'i2'),           # Channel 4 digitization rate
                      ('lockout_index_chan1', 'i2'),        # Channel 1 lockout index
                      ('lockout_index_chan2', 'i2'),        # Channel 2 lockout index
                      ('lockout_index_chan3', 'i2'),        # Channel 3 lockout index
                      ('lockout_index_chan4', 'i2'),        # Channel 4 lockout index
                      ('bins_chan1', 'i2'),                 # Channel 1 number of bins
                      ('bins_chan2', 'i2'),                 # Channel 2 number of bins
                      ('bins_chan3', 'i2'),                 # Channel 3 number of bins
                      ('bins_chan4', 'i2'),                 # Channel 4 number of bins
                      ('range_samples_chan1', 'i2'),        # Channel 1 range samples
                      ('range_samples_chan2', 'i2'),        # Channel 2 range samples
                      ('range_samples_chan3', 'i2'),        # Channel 3 range samples
                      ('range_samples_chan4', 'i2'),        # Channel 4 range samples
                      ('ping_per_profile', 'i2'),           # Number of pings per profile
                      ('averaged_pings', 'i2'),             # Flag to indicate if pings are averaged in time
                      ('num_acquired_pings', 'i2'),         # Pings that have been acquired in this burst
                      ('ping_period', 'i2'),                # Pings period in seconds
                      ('first_ping', 'i2'),                 # Number of the first averaged ping
                      ('last_ping', 'i2'),                  # Number of the last averaged ping
                      ('data_type_chan1', 'i1'),            # Chan 1 data type. 1 = averaged data, 0 = not averaged data
                      ('data_type_chan2', 'i1'),            # Chan 2 data type. 1 = averaged data, 0 = not averaged data
                      ('data_type_chan3', 'i1'),            # Chan 3 data type. 1 = averaged data, 0 = not averaged data
                      ('data_type_chan4', 'i1'),            # Chan 4 data type. 1 = averaged data, 0 = not averaged data
                      ('data_error', 'i2'),                 # Error number
                      ('phase', 'i1'),                      # Phase used to acquired this profile
                      ('over_run', 'i1'),                   # 1 if over run occurred
                      ('num_channels', 'i1'),               # 1,2,3 or 4
                      ('gain_chan1', 'i1'),                 # Gain 0,1,2,3
                      ('gain_chan2', 'i1'),                 # Gain 0,1,2,3
                      ('gain_chan3', 'i1'),                 # Gain 0,1,2,3
                      ('gain_chan4', 'i1'),                 # Gain 0,1,2,3
                      ('spare', 'i1'),                      # spare
                      ('pulse_length_chan1', 'i2'),         # Chan 1 Pulse length
                      ('pulse_length_chan2', 'i2'),         # Chan 2 Pulse length
                      ('pulse_length_chan3', 'i2'),         # Chan 3 Pulse length
                      ('pulse_length_chan4', 'i2'),         # Chan 4 Pulse length
                      ('board_number_chan1', 'i2'),         # The board the data came from for channel 1
                      ('board_number_chan2', 'i2'),         # The board the data came from for channel 2
                      ('board_number_chan3', 'i2'),         # The board the data came from for channel 3
                      ('board_number_chan4', 'i2'),         # The board the data came from for channel 4
                      ('board_freq_chan1', 'i2'),           # The frequency for the channel 1
                      ('board_freq_chan2', 'i2'),           # The frequency for the channel 2
                      ('board_freq_chan3', 'i2'),           # The frequency for the channel 3
                      ('board_freq_chan4', 'i2'),           # The frequency for the channel 4
                      ('sensor_flag', 'i2'),                # Flag to indicate if pressure sensor or temp sensor avail
                      ('tilt_x', 'u2'),                     # Counts
                      ('tilt_y', 'u2'),                     # Counts
                      ('battery', 'u2'),                    # Counts
                      ('pressure', 'u2'),                   # Counts
                      ('temperature', 'i2'),                # Counts
                      ('ad_channel_6', 'u2'),               # AD channel 6
                      ('ad_channel_7', 'u2')])              # AD channel 7

sample_dtype = sample_dtype.newbyteorder('>')               # Data is in Big Endian


class ZplscCParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    INTERNAL_TIME_STAMP = 'internal_time_stamp'
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'

    FILE_TIME = "zplsc_timestamp"               # raw file timestamp
    FILE_NAME = "zplsc_echogram"                # output echogram plot .png/s path and filename
    CHANNEL = "zplsc_channel"
    TRANSDUCER_DEPTH = "zplsc_transducer_depth" # five digit floating point number (%.5f, in meters)
    FREQUENCY = "zplsc_frequency"               # six digit fixed point integer (in Hz)
    TRANSMIT_POWER = "zplsc_transmit_power"     # three digit fixed point integer (in Watts)
    PULSE_LENGTH = "zplsc_pulse_length"         # six digit floating point number (%.6f, in seconds)
    BANDWIDTH = "zplsc_bandwidth"               # five digit floating point number (%.5f in Hz)
    SAMPLE_INTERVAL = "zplsc_sample_interval"   # six digit floating point number (%.6f, in seconds)
    SOUND_VELOCITY = "zplsc_sound_velocity"     # five digit floating point number (%.5f, in m/s)
    ABSORPTION_COEF = "zplsc_absorption_coeff"  # four digit floating point number (%.4f, dB/m)
    TEMPERATURE = "zplsc_temperature"           # three digit floating point number (%.3f, in degC)


# The following is used for _build_parsed_values() and defined as below:
# (parameter name, encoding function)
ZPLSC_DATA_ENCODING_RULES = [
    (ZplscCParticleKey.INTERNAL_TIME_STAMP,           str),
    (ZplscCParticleKey.DCL_CONTROLLER_TIMESTAMP,      lambda x: [str(y) for y in x]),

    (ZplscCParticleKey.FILE_TIME,           str),
    (ZplscCParticleKey.FILE_NAME,           lambda x: [str(y) for y in x]),
    (ZplscCParticleKey.CHANNEL,             lambda x: [int(y) for y in x]),
    #(ZplscCParticleKey.TRANSDUCER_DEPTH,    lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.FREQUENCY,           lambda x: [float(y) for y in x]),
    #(ZplscCParticleKey.TRANSMIT_POWER,      lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.PULSE_LENGTH,        lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.BANDWIDTH,           lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.SAMPLE_INTERVAL,     lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.SOUND_VELOCITY,      lambda x: [float(y) for y in x]),
    #(ZplscCParticleKey.ABSORPTION_COEF,     lambda x: [float(y) for y in x]),
    (ZplscCParticleKey.TEMPERATURE,         lambda x: [float(y) for y in x]),
]


class DataParticleType(BaseEnum):
    ZPLSC_C_DCL_SAMPLE = 'zplsc_c_dcl_sample'


class ZplscCInstrumentDataParticle(DataParticle):
    """
    Class for generating the zplsc_c_dcl instrument particle.
    """

    _data_particle_type = DataParticleType.ZPLSC_C_DCL_SAMPLE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], function)
                for name, function in ZPLSC_DATA_ENCODING_RULES]


class ZplscCDclParser(SimpleParser):
    """
    ZPLSC C DCL Parser.
    """

    __metaclass__ = get_logging_metaclass(log_level='debug')

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def __init__(self, config, stream_handle, exception_callback, output_file_path):
        """
        Initialize the ZplscCDcl parser, which does not use state or the chunker
        and sieve functions.
        @param config: The parser configuration dictionary
        @param stream_handle: The stream handle of the file to parse
        @param exception_callback: The callback to use when an exception occurs
        @param output_file_path: The location to output the echogram plot .png files
        """

        self.output_file_path = output_file_path
        self._chan_frequency = dict()

        with open(os.path.join(RESOURCE_PATH, DPL_FILE_NAME), mode='r') as f:
            dlp_data = f.read()

        if dlp_data:
            match = SERIAL_NUMBER_MATCHER.search(dlp_data)
            if match:
                self._serial_number = match.groups()[0]
                log.info('Found instrument serial number %s', self._serial_number)
            else:
                self.recov_exception_callback("Serial number is not found in the deployment file")

            match = CHAN1_FREQUENCY_MATCHER.search(dlp_data)
            if match:
                self._chan_frequency[CHANNEL_1] = int(match.groups()[0]) * KHZ
            else:
                self.recov_exception_callback("Channel 1 Frequency is not found in the deployment file")

            match = CHAN2_FREQUENCY_MATCHER.search(dlp_data)
            if match:
                self._chan_frequency[CHANNEL_2] = int(match.groups()[0]) * KHZ
            else:
                self.recov_exception_callback("Channel 2 Frequency is not found in the deployment file")

            match = CHAN3_FREQUENCY_MATCHER.search(dlp_data)
            if match:
                self._chan_frequency[CHANNEL_3] = int(match.groups()[0]) * KHZ
            else:
                self.recov_exception_callback("Channel 3 Frequency is not found in the deployment file")

            match = CHAN4_FREQUENCY_MATCHER.search(dlp_data)
            if match:
                self._chan_frequency[CHANNEL_4] = int(match.groups()[0]) * KHZ
            else:
                self.recov_exception_callback("Channel 4 Frequency is not found in the deployment file")

            match = SOUND_SPEED_MATCHER.search(dlp_data)
            if match:
                self._sound_speed = float(match.groups()[0])
            else:
                self.recov_exception_callback("Sound speed is not found in the deployment file")

        super(ZplscCDclParser, self).__init__(config, stream_handle, exception_callback)

    def parse_dcl(self, line):
        """
        Parse DCL line for timestamp
        """

        dcl_power_on_match = DCL_POWER_ON_MATCHER.match(line)
        if dcl_power_on_match:
            group_timestamp = dcl_power_on_match.groups()[SENSOR_GROUP_TIMESTAMP]

        dcl_log_match = DCL_LOG_MATCHER.match(line)
        if dcl_log_match:
            group_timestamp = dcl_log_match.groups()[SENSOR_GROUP_TIMESTAMP]

        dcl_senor_log_match = SENSOR_TIME_MATCHER.match(line)
        if dcl_senor_log_match:
            time_stamp = dcl_senor_log_match.groups()[TIMESTAMP]
            unit = dcl_senor_log_match.groups()[UINT]

    def parse_file(self):
        """
        Parse the zplsc c dcl binary file.
        """

        # Extract the full file time from the stream handle
        input_file_name = self._stream_handle.name
        (file_path, file_name) = os.path.split(input_file_name)

        # Tuple contains the string before the '.', the '.', and the '04A' string
        outfile = file_name.rpartition('.')[0]
        file_date = outfile[:6]

        position = 0
        zplsc_data = defaultdict(list)

        trans_keys = range(1, MAX_CHANNEL+1)
        trans_array = dict((key, []) for key in trans_keys)         # transducer power data
        trans_array_time = dict((key, []) for key in trans_keys)    # transducer time data
        td_f = dict.fromkeys(trans_keys)                            # transducer frequency
        td_dR = dict.fromkeys(trans_keys)                           # transducer depth measurement
        pulse_length = dict()

        # Read binary file a block at a time
        raw = self._stream_handle.read(BLOCK_SIZE)

        while raw:
            # We only care for the Sample datagram, skip over all the other data
            match = SAMPLE_MATCHER.search(raw)
            if not match:
                log.info("Profile data does not match...")
            else:
                match_start = match.start()

                # Seek to the position of the binary flag and read the Sample Datagram into numpy array
                self._stream_handle.seek(position + match_start)
                sample_data = np.fromfile(self._stream_handle, dtype=sample_dtype, count=1)

                burst_number = sample_data['burst_number'][0]
                burst_interval = sample_data['burst_interval'][0]

                # Extract various calibration parameters used for generating echogram plot
                if not td_dR:
                    for channel in td_f.iterkeys():
                        td_f[channel] = self._chan_frequency[channel]
                        td_dR[channel] = self._sound_speed * burst_interval / 2

                        log.info("td_f %d = %d", channel, td_f[channel])

                serial_number = sample_data['serial_number'][0]
                ping_status = sample_data['ping_status'][0]

                year = sample_data['year'][0]
                month = sample_data['month'][0]
                day = sample_data['day'][0]
                hour = sample_data['hour'][0]
                minute = sample_data['minute'][0]
                second = sample_data['second'][0]
                hundreds = sample_data['hundreds'][0]

                chan1_digit_rate = sample_data['digit_rate_chan1'][0]
                chan2_digit_rate = sample_data['digit_rate_chan2'][0]
                chan3_digit_rate = sample_data['digit_rate_chan3'][0]
                chan4_digit_rate = sample_data['digit_rate_chan4'][0]
                chan1_lockout_index = sample_data['lockout_index_chan1'][0]
                chan2_lockout_index = sample_data['lockout_index_chan2'][0]
                chan3_lockout_index = sample_data['lockout_index_chan3'][0]
                chan4_lockout_index = sample_data['lockout_index_chan4'][0]
                chan1_bins = sample_data['bins_chan1'][0]
                chan2_bins = sample_data['bins_chan2'][0]
                chan3_bins = sample_data['bins_chan3'][0]
                chan4_bins = sample_data['bins_chan4'][0]
                chan1_range_sample = sample_data['range_samples_chan1'][0]
                chan2_range_sample = sample_data['range_samples_chan2'][0]
                chan3_range_sample = sample_data['range_samples_chan3'][0]
                chan4_range_sample = sample_data['range_samples_chan4'][0]

                ping_per_profile = sample_data['ping_per_profile'][0]
                averaged_pings = sample_data['averaged_pings'][0]
                num_acquired_pings = sample_data['num_acquired_pings'][0]
                ping_period = sample_data['ping_period'][0]
                first_ping = sample_data['first_ping'][0]
                last_ping = sample_data['last_ping'][0]
                chan1_data_type = sample_data['data_type_chan1'][0]
                chan2_data_type = sample_data['data_type_chan2'][0]
                chan3_data_type = sample_data['data_type_chan3'][0]
                chan4_data_type = sample_data['data_type_chan4'][0]
                data_error = sample_data['data_error'][0]
                phase = sample_data['phase'][0]
                over_run = sample_data['over_run'][0]
                num_channels = sample_data['num_channels'][0]
                chan1_gain = sample_data['gain_chan1'][0]
                chan2_gain = sample_data['gain_chan2'][0]
                chan3_gain = sample_data['gain_chan3'][0]
                chan4_gain = sample_data['gain_chan4'][0]
                spare = sample_data['spare'][0]

                chan1_pulse_length = sample_data['pulse_length_chan1'][0]
                chan2_pulse_length = sample_data['pulse_length_chan2'][0]
                chan3_pulse_length = sample_data['pulse_length_chan3'][0]
                chan4_pulse_length = sample_data['pulse_length_chan4'][0]
                chan1_board_freq = sample_data['board_number_chan1'][0]
                chan2_board_freq = sample_data['board_number_chan2'][0]
                chan3_board_freq = sample_data['board_number_chan3'][0]
                chan4_board_freq = sample_data['board_number_chan4'][0]
                sensor_flag = sample_data['sensor_flag'][0]
                tilt_x = sample_data['tilt_x'][0]
                tilt_y = sample_data['tilt_y'][0]
                battery = sample_data['battery'][0]
                pressure = sample_data['pressure'][0]
                temperature = sample_data['temperature'][0]
                ad_channel_6 = sample_data['ad_channel_6'][0]
                ad_channel_7 = sample_data['ad_channel_7'][0]

                log.info("c1 pulse length = %d, c2 pl = %d, c3 pl = %d, c4 pl = %d", chan1_pulse_length, chan2_pulse_length, chan3_pulse_length, chan4_pulse_length)
                log.info("burst number %d, serial number %d, burst interval %d", burst_number, serial_number, burst_interval)
                log.info("year = %d, month = %d, day = %d", year, month, day)
                log.info('hour = %d, minutes = %d, second = %d, hundreds = %d', hour, minute, second, hundreds)
                log.info("c1 digit rate  =  %d, c2 digit rate = %d, c3 drate =  %d, c4 drate = %d", chan1_digit_rate, chan2_digit_rate, chan3_digit_rate, chan4_digit_rate)
                log.info("c1_lockout = %d, c2_lockout = %d, c3_lockout = %d, c4_lockout = %d", chan1_lockout_index, chan2_lockout_index, chan3_lockout_index, chan4_lockout_index)
                log.info("c1_bins = %d, c2_bins = %d, c3_bins = %d, c4_bins = %d", chan1_bins, chan2_bins, chan3_bins, chan4_bins)
                log.info('chan1 range sample = %d c2 rs = %d, c3 rs = %d, c4 rs = %d', chan1_range_sample, chan2_range_sample, chan3_range_sample, chan4_range_sample)
                log.info("ping per profile = %d, averaged pings = %d, num acquired pings  = %d, ping period = %d", ping_per_profile, averaged_pings, num_acquired_pings, ping_period)
                log.info("first ping = %d, last ping = %d ", first_ping, last_ping)
                log.info("c1_data_type = %s, c2_data_type = %s, c3_data_type = %s, c4_data_type = %s,", chan1_data_type, chan2_data_type, chan3_data_type, chan4_data_type)
                log.info("c1_gain = %d, c2_gain = %d, c3_gain = %d, c4_gain = %d", chan1_gain, chan2_gain, chan3_gain, chan4_gain)
                log.info("c1 board freq = %d, c2 bf = %d, c3 bf = %d, c4 bf = %d", chan1_board_freq, chan2_board_freq, chan3_board_freq, chan4_board_freq)
                log.info("sensor flag = %d, tilt_x = %d, tilt_y = %d, battery = %d, pressure = %d", sensor_flag, tilt_x, tilt_y, battery, pressure)
                log.info("temperature = %d, ad_channel 6 = %d, ad_channel 7 = %d", temperature, ad_channel_6, ad_channel_7)

                micro_seconds = hundreds*1e4;
                ordinal_time = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), int(micro_seconds)).toordinal()
                unix_time = calendar.timegm((year, month, day, hour, minute, second+(hundreds/1e2)))
                time_stamp = ntplib.system_to_ntp_time(unix_time)

                log.info("year = %d, month = %d, day = %d, hour = %d, minute = %d, second = %d, hundreds = %d, micro = %d", \
                         year, month, day, hour, minute, second, hundreds, micro_seconds)
                log.info('time_stamp = %d', time_stamp)

                # Collect pulse length only once for metadata
                if not pulse_length:
                    log.info("pulse length is empty")
                    pulse_length[CHANNEL_1] = sample_data['pulse_length_chan1'][0]
                    pulse_length[CHANNEL_2] = sample_data['pulse_length_chan2'][0]
                    pulse_length[CHANNEL_3] = sample_data['pulse_length_chan3'][0]
                    pulse_length[CHANNEL_4] = sample_data['pulse_length_chan4'][0]

                # Gather metadata once per channel number
                for channel in td_f.iterkeys():
                    if not trans_array[channel]:
                        td_f[channel] = self._chan_frequency[channel]

                        td_dR[channel] = self._sound_speed * burst_interval / 2
                        file_name = self.output_file_path + '/' + outfile + '_' + \
                                str(int(self._chan_frequency[channel])/KHZ) + 'k.png'
                        file_time =  str(hour) + str(minute) + str(second)

                        log.info("td_f %d = %d", channel, td_f[channel])
                        log.info('file time = %s', file_time)

                        zplsc_data[ZplscCParticleKey.FILE_TIME] = file_time
                        zplsc_data[ZplscCParticleKey.FILE_NAME].append(file_name)
                        zplsc_data[ZplscCParticleKey.CHANNEL].append(channel)
                        zplsc_data[ZplscCParticleKey.FREQUENCY].append(self._chan_frequency[channel])
                        zplsc_data[ZplscCParticleKey.PULSE_LENGTH].append(pulse_length[channel])

                        #zplsc_data[ZplscCParticleKey.TRANSMIT_POWER].append(sample_data['transmit_power'][0])
                        #zplsc_data[ZplscCParticleKey.TRANSDUCER_DEPTH].append(sample_data['transducer_depth'][0])
                        #zplsc_data[ZplscCParticleKey.TRANSMIT_POWER].append(sample_data['transmit_power'][0])
                        #zplsc_data[ZplscCParticleKey.TRANSDUCER_DEPTH].append(sample_data['transducer_depth'][0])
                        #zplsc_data[ZplscCParticleKey.BANDWIDTH].append(sample_data['bandwidth'][0])
                        #zplsc_data[ZplscCParticleKey.ABSORPTION_COEF].append(sample_data['absorption_coefficient'][0])

                        zplsc_data[ZplscCParticleKey.SAMPLE_INTERVAL].append(sample_data['burst_interval'][0])
                        zplsc_data[ZplscCParticleKey.SOUND_VELOCITY].append(self._sound_speed)
                        zplsc_data[ZplscCParticleKey.TEMPERATURE].append(sample_data['temperature'][0])

                        # Extract a particle and append it to the record buffer
                        if channel == MAX_CHANNEL:
                            particle = self._extract_sample(ZplscCInstrumentDataParticle, None, zplsc_data, time_stamp)
                            self._record_buffer.append(particle)

                            log.info('Parsed particle: %s', particle.generate_dict())

                # Setup the numpy averaged data and overflow data type
                averaged_data_dtype = np.dtype([('averaged_data', 'u4')])
                overflow_counts_dtype = np.dtype([('overflow_counts', 'u1')])

                bins_chan1 = sample_data['bins_chan1'][0]
                # Read Channel 1 averaged data and overflow counts
                if bins_chan1 > 0:
                    chan1_averaged_data = np.fromfile(self._stream_handle, dtype=averaged_data_dtype, count=bins_chan1)
                    chan1_overflow_counts = np.fromfile(self._stream_handle, dtype=overflow_counts_dtype, count=bins_chan1)
                    trans_array_time[CHANNEL_1].append(ordinal_time)
                    trans_array[CHANNEL_1].append(chan1_averaged_data)

                bins_chan2 = sample_data['bins_chan2'][0]
                # Read Channel 2 averaged data and overflow counts
                if bins_chan2 > 0:
                    chan2_averaged_data = np.fromfile(self._stream_handle, dtype=averaged_data_dtype, count=bins_chan2)
                    chan2_overflow_counts = np.fromfile(self._stream_handle, dtype=overflow_counts_dtype, count=bins_chan2)
                    trans_array_time[CHANNEL_2].append(ordinal_time)
                    trans_array[CHANNEL_2].append(chan2_averaged_data)

                bins_chan3 = sample_data['bins_chan3'][0]
                # Read Channel 3 averaged data and overflow counts
                if bins_chan3 > 0:
                    chan3_averaged_data = np.fromfile(self._stream_handle, dtype=averaged_data_dtype, count=bins_chan3)
                    chan3_overflow_counts = np.fromfile(self._stream_handle, dtype=overflow_counts_dtype, count=bins_chan3)
                    trans_array_time[CHANNEL_3].append(ordinal_time)
                    trans_array[CHANNEL_3].append(chan3_averaged_data)

                bins_chan4 = sample_data['bins_chan4'][0]
                # Read Channel 4 averaged data and overflow count
                if bins_chan4 > 0:
                    chan4_averaged_data = np.fromfile(self._stream_handle, dtype=averaged_data_dtype, count= bins_chan4)
                    chan4_overflow_counts = np.fromfile(self._stream_handle, dtype=overflow_counts_dtype, count=bins_chan4)
                    trans_array_time[CHANNEL_4].append(ordinal_time)
                    trans_array[CHANNEL_4].append(chan4_averaged_data)

                log.info('chan1_averaged_data ')
                log.info('%s', chan1_averaged_data)
                log.info('chan1_overflow_counts ')
                log.info('%s', chan1_overflow_counts)

                log.info('chan2_averaged_data ')
                log.info('%s', chan2_averaged_data)
                log.info('chan2_overflow_counts ')
                log.info('%s', chan2_overflow_counts)

                log.info('chan3_averaged_data ')
                log.info('%s', chan3_averaged_data)
                log.info('chan3_overflow_counts ')
                log.info('%s', chan3_overflow_counts)

                log.info('chan4_averaged_data ')
                log.info('%s', chan4_averaged_data)
                log.info('chan4_overflow_counts ')
                log.info('%s', chan4_overflow_counts)

                # for x in xrange(len(chan1_averaged_data)):
                #     log.info(' chan1 averaged data[%d] = %d', x, chan1_averaged_data['averaged_data'][x] )
                #

            # Need current position in file to increment for next regex search offset
            position = self._stream_handle.tell()

            # Read the next block for regex search
            raw = self._stream_handle.read(BLOCK_SIZE)

        # Driver spends most of the time plotting,
        # this can take longer for more transducers so lets break out the work
        processes = []
        for channel in td_f.iterkeys():
            try:
                log.info("trans_array_time ****** = %s", trans_array_time[channel])
                log.info("trans_array channel ****** = %s", trans_array[channel])
                process = Process(target=self.generate_echogram_plot,
                                  args=(trans_array_time[channel], trans_array[channel],
                                        td_f[channel], td_dR[channel], channel,
                                        zplsc_data[ZplscCParticleKey.FILE_NAME][channel-1]))
                process.start()
                processes.append(process)

            except Exception, e:
                log.error("Error: Unable to start process: %s", e)

        for p in processes:
            p.join()

    @staticmethod
    def generate_echogram_plot(trans_array_time, trans_array, td_f, td_dR, channel, filename):
        # Generate echogram plots with sample data collected for each channel
        # Transpose array data so the sample power data is on the y-axis
        trans_array = np.transpose(trans_array)

        generate_plots(trans_array, trans_array_time, td_f, td_dR,
                       "Transducer # " + str(channel) + ": ", filename)
