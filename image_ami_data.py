#!/usr/bin/env python
"""An integration test to ensure that the ami_quicklook routine is working"""
import os
import logging
import sys
import multiprocessing
from autocrunch import ami_rawfile_quicklook

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                    level=logging.DEBUG)

default_output_dir = os.environ.get('AUTOCRUNCH_OUTPUT_DIR',
                                    os.path.expanduser("~/ami_test"))
default_ami_dir = os.environ.get('AUTOCRUNCH_AMI_DIR',
                                 '/home/amicamb/ami')
default_casa_dir = os.environ.get('AUTOCRUNCH_CASA_DIR',
                              '/home/soft/misc/builds/casa-release-4.5.0-el6')
# default_casa_dir = '/opt/soft/builds/casapy-active'
nthreads = 4


def processed_callback(summary):
    logger.info('*** Job complete: ' + summary)


if __name__ == "__main__":
    logging.basicConfig(format='%(name)s:%(message)s',
                        filemode='w',
                        filename="ami-crunch.log",
                        level=logging.DEBUG)
    log_stdout = logging.StreamHandler(sys.stdout)
    log_stdout.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.addHandler(log_stdout)

    args1 = ['V404CYG-160121.raw',
             default_ami_dir,
             default_casa_dir,
             default_output_dir]

    args2 = ['SSCYG-160202.raw',
             default_ami_dir,
             default_casa_dir,
             default_output_dir]

    # return_message = ami_rawfile_quicklook(*args1)
    # print return_message

    pool = multiprocessing.Pool(2)
    return_message1 = pool.apply_async(ami_rawfile_quicklook,
                                      args=args1, callback=processed_callback)
    return_message2 = pool.apply_async(ami_rawfile_quicklook,
                                      args=args2, callback=processed_callback)
    print return_message1.get(timeout=1200)
    print return_message2.get(timeout=1200)

    #    for listing in single_file_listings:
    #        result = pool.apply_async(process_dataset, [listing])
    #    print result.get(timeout=1200)
    print "Fin."
