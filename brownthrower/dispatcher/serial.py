#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import yaml

from brownthrower import common
from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants


# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.task'  : 'brownthrower.task',
    'entry_points.event' : 'brownthrower.event',
    'manager.editor'     : 'nano',
    'database.url'       : 'postgresql://tallada:secret,@db01.pau.pic.es/test_tallada',
}

log = logging.getLogger('brownthrower.dispatcher.serial')
# TODO: Remove
logging.basicConfig(level=logging.DEBUG)

class SerialDispatcher(interface.Dispatcher):
    
    _tasks = {}
    
    def __init__(self, *args, **kwargs):
        # TODO:Fallar si no hi ha tasks
        self._tasks = common.load_tasks(_CONFIG['entry_points.task'])
    
    def loop(self):
        try:
            while True:
                # Fetch first job which WAS suitable to be executed
                job = model.session.query(model.Job).filter(
                    model.Job.status == constants.JobStatus.READY,
                    model.Job.task.in_(self._tasks.keys()),
                    ~ model.Job.parent_jobs.any( #@UndefinedVariable
                        model.Job.status != constants.JobStatus.DONE,
                    )
                ).with_lockmode('update').first()
                
                if not job:
                    log.info("No more jobs are ready to be executed. Exitting...")
                    model.session.rollback()
                    break
                
                # Recheck parents to see if it is still runnable
                parents = model.session.query(model.Job).filter(
                    model.Job.child_jobs.contains(job) #@UndefinedVariable
                ).with_lockmode('read').all()
                if filter(lambda parent: parent.status != constants.JobStatus.DONE, parents):
                    log.debug("Some parents of this job have changed its status before being locked. This job is now not ready to be executed. Skipping...")
                    model.session.rollback()
                    continue
                
                try:
                    job.input = yaml.dump([ yaml.load(parent.input) for parent in parents ])
                except:
                    log.error("Could not build the input for this job. SKipping...")
                
                try:
                
                #job.status = constants.JobStatus.RUNNING
                model.session.commit()
                try:
                    out = 
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
        #except:
            # sortir be
        #    pass
        finally:
            model.session.rollback()
        