#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 17:26:56 2020

@author: tpd
"""

import os

_dir = os.path.abspath(os.path.dirname(__file__))
_par_dir, _ = os.path.split(_dir)

_logs = os.path.join(_par_dir, 'logs')
if not os.path.exists(_logs):
    os.mkdir(_logs)
