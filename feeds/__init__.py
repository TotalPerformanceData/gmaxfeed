#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 17:26:56 2020

@author: tpd
"""

import os
import sys
import logging

# to initialise subpackages which aren't automatically imported by just the top level
import dateutil
import dateutil.tz
import dateutil.parser
import logging.handlers


if "MAIN_DIR" in os.environ:
    if os.environ["MAIN_DIR"] not in sys.path:
        sys.path.append(os.environ["MAIN_DIR"])

if "LOGS_DIR" in os.environ:
    LOGS_DIR = os.environ["LOGS_DIR"]
else:
    LOGS_DIR = os.path.join(os.path.split(os.path.abspath(os.path.dirname(__file__)))[0], "logs")
if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)


def get_logger(name:str, level = logging.INFO, handler_only: bool = False) -> logging.RootLogger:
    """
    function to return a new logger, as intend to have one logger for each file
    Inputs:
        name: str - the name for this logger, usually __name__ 
        level: the level of the log, such as logging.INFO, .WARNING, etc
        handler_only: bool - whtehr to return the handler only instead of full logger
    Outputs:
        logger - instance of logging.RootLogger
    """
    ch = logging.handlers.RotatingFileHandler(filename = os.path.join(LOGS_DIR, os.path.split(name)[1] + ".log"),
                                              maxBytes = 10**7,
                                              backupCount = 10)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - File:%(filename)s - Line:%(lineno)d - %(levelname)s | %(message)s")
    ch.setFormatter(formatter)
    if handler_only:
        return ch
    else:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(ch)
        return logger

