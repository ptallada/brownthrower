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
        # TODO: Fallar si no hi ha tasks
        self._tasks = common.load_tasks(_CONFIG['entry_points.task'])
    
    def loop(self):
        while True:
            try:
                # Fetch first job which WAS suitable to be executed
                job = model.session.query(model.Job).filter(
                    model.Job.status == constants.JobStatus.READY,
                    model.Job.task.in_(self._tasks.keys()),
                    ~ model.Job.parent_jobs.any( #@UndefinedVariable
                        model.Job.status != constants.JobStatus.DONE,
                    )
                ).with_lockmode('update').first()
                
                if not job:
                    log.info("There are no more jobs suitable to be executed.")
                    break
                
                # Re-check parents to see if it is still runnable
                parents = model.session.query(model.Job).filter(
                    model.Job.child_jobs.contains(job) #@UndefinedVariable
                ).with_lockmode('read').all()
                if filter(lambda parent: parent.status != constants.JobStatus.DONE, parents):
                    log.debug("Skipping this job as some of its parents have changed its status before being locked.")
                    continue
                
                try:
                    with model.session.begin_nested():
                        job.input = yaml.dump([ yaml.load(parent.output) for parent in parents ])
                        
                        task = self._tasks[job.task]
                        task.check_config(job.config)
                        task.check_input(job.input)
                        
                        # Cache fields for using after commit
                        job_id     = job.id
                        job_config = job.config
                        job_input  = job.input
                        
                        job.status = constants.JobStatus.RUNNING
                except:
                    try:
                        raise
                    except interface.TaskValidationException:
                        log.error("The input or the config is not valid.")
                    except yaml.YAMLError:
                        log.error("The output of some of its parents is not valid.")
                    except model.StatementError:
                        log.error("Could not complete the query to the database.")
                    finally:
                        job.status = constants.JobStatus.SUBMIT_FAIL
                        continue
                finally:
                    # Job is now RUNNING or SUBMIT_FAIL
                    model.session.commit()
                
                # The job is now RUNNING
                try:
                    with model.session.begin_nested():
                        job_output = task.run(runner = None, config = job_config, inp = job_input)
                        
                        job = model.session.query(model.Job).filter_by(
                            id = job_id
                        ).with_lockmode('update').one()
                        
                        if job.status == constants.JobStatus.CANCEL:
                            raise interface.TaskCancelledException()
                        
                        job.output = job_output
                        task.check_output(job.output)
                        
                        job.status = constants.JobStatus.DONE
                except:
                    try:
                        raise
                    except interface.TaskCancelledException:
                        log.info("The job was cancelled.")
                    except interface.TaskValidationException:
                        log.error("The output is not valid.")
                    except model.StatementError:
                        log.error("Could not commit changes to the database.")
                    except Exception:
                        log.error("The job raised an Exception.")
                    finally:
                        job.status = constants.JobStatus.FAILED
                finally:
                    # Job is now DONE or cancelled => FAILED
                    model.session.commit()
            finally:
                # Unlock the job if it was skipped
                model.session.rollback()
