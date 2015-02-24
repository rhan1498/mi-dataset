#!/usr/bin/env python

"""
@package mi.dataset.driver.zplsc_a
@file mi-dataset/mi/dataset/driver/zplsc_a/zplsc_c_dcl_driver.py
@author Richard Han
@brief DCL driver for the zplsc_c instrument
Release notes:
Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.zplsc_c_dcl import ZplscCDclParser


def parse(basePythonCodePath, sourceFilePath, outputFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param outputFilePath This is the full path of echogram files to be stored
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        ZplscCDclDriver(basePythonCodePath, stream_handle, outputFilePath,
                                particleDataHdlrObj).processFileStream()

    return particleDataHdlrObj


class ZplscCDclDriver(SimpleDatasetDriver):
    """
    The zplsc_c_dcl driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, basePythonCodePath, stream_handle, outputFilePath, particleDataHdlrObj):

        self.outputFilePath = outputFilePath

        super(ZplscCDclDriver, self).__init__(basePythonCodePath, stream_handle, particleDataHdlrObj)

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.zplsc_c_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'ZplscCInstrumentDataParticle'}

        parser = ZplscCDclParser(parser_config,
                              stream_handle,
                              self._exception_callback,
                              self.outputFilePath)

        return parser
