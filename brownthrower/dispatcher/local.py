#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from brownthrower import model

# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.task'  : 'paudm.task',
    'entry_points.event' : 'paudm.event',
    'manager.editor'     : 'nano',
    'database.url'       : 'postgresql://tallada:secret,@db01.pau.pic.es/test_tallada',
}

log = logging.getLogger('paudm.manager')
# TODO: Remove
logging.basicConfig(level=logging.DEBUG)

class LocalDispatcher(object):
    def loop(self):
        try:
            while True:
                # get del primer job llest per executar
                # bloquejar-lo
                # si no n'hi ha sortir. HEM ACABAT
                try:
                    # corre'l
                    # rebre output, desar-lo
                    # actualitzar estat
                    # commit
                    pass
                except:
                    # rollback
                    # marcar com a fallat
                    # commit
                    pass
        except KeyboardInterrupt:
            # sortir be
            pass