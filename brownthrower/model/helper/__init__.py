#!/usr/bin/env python
# -*- coding: utf-8 -*-

def get_helper(drivername):
    return __import__(drivername, globals(), locals(), [], -1).Helper
