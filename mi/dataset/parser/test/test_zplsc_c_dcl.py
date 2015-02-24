#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_zplsc_c_dcl
@file marine-integrations/mi/dataset/parser/test/test_zplsc_c_dcl.py
@author Richard Han (Raytheon)
@brief Test code for a zplsc_c_dcl data parser

Files used for testing:

14102414-2.04A

"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger;
from mi.dataset.parser.zplsc_c_dcl import ZplscCDclParser

log = get_logger()

from mi.core.exceptions import DatasetParserException
from mi.core.instrument.data_particle import DataParticleKey

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


from mi.idk.config import Config
RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'zplsc_a', 'dcl', 'resource')

MODULE_NAME = 'mi.dataset.parser.zplsc_c_dcl'

FILE1 = '14102414-2.04A'


@attr('UNIT', group='mi')
class ZplscCDclParserUnitTestCase(ParserUnitTestCase):
    """
    zplsc_c_dcl Parser unit test suite
    """

    def create_zplsc_c_dcl_parser(self, file_handle, filename):
        """
        This function creates a ZplscCDCL parser for recovered data.
        """
        # For testing, just output to the same test directory.
        # The real output file path will be set in the spring xml file.
        outputFilePath = os.path.join('mi', 'dataset', 'driver', 'zplsc_b', 'test')
        return ZplscCDclParser(self.rec_config, file_handle, self.rec_exception_callback, outputFilePath)


    def open_file(self, filename):
        log.info('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return open(os.path.join(RESOURCE_PATH, filename), mode='r')


    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1


    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0

        self.tel_exception_callback_value = None
        self.tel_exceptions_detected = 0

    def test_zplsc_c_dcl_parser(self):
        """
        Test Zplsc C DCL parser
        """
        log.debug('===== START TEST ZPLSC_C_DCL Parser =====')

        in_file = self.open_file(FILE1)

        parser = self.create_zplsc_c_dcl_parser(in_file, FILE1)
        parser.parse_file()

        in_file.close()

        log.debug('===== END TEST ZPLSC_C_DCL Parser  =====')


    def create_large_yml(self):
        """
        Create a large yml file corresponding to an actual recovered dataset. This is not an actual test - it allows
        us to create what we need for integration testing, i.e. a yml file.
        """

        in_file = self.open_file(FILE1)
        parser = self.create_rec_parser(in_file, FILE1)

        # In a single read, get all particles in this file.
        result = parser.get_records(1000)

        self.particle_to_yml(result, 'Richard_zplsc_rec_01.yml')


    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)
        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i+1))
            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.5f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()
