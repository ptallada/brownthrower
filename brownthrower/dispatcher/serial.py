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
                    log.info("There are no more jobs to be executed.")
                    break
                
                # Recheck parents to see if it is still runnable
                parents = model.session.query(model.Job).filter(
                    model.Job.child_jobs.contains(job) #@UndefinedVariable
                ).with_lockmode('read').all()
                if filter(lambda parent: parent.status != constants.JobStatus.DONE, parents):
                    log.debug("Skipping this job as some of its parents have changed its status before being locked.")
                    continue
                
                try:
                    job.input = yaml.dump([ yaml.load(parent.output) for parent in parents ])
                    
                    task = self._tasks[job.task]
                    task.check_config(job.config)
                    task.check_input(job.input)
                    
                    # Prefetch
                    job_id     = job.id
                    job_config = job.config
                    job_input  = job.input
                    
                    job.status = constants.JobStatus.RUNNING
                    model.session.commit()
                    try:
                        job_output = task.run(runner = None, config = job_config, inp = job_input)
                        
                        job = model.session.query(model.Job).filter_by(
                            id = job_id
                        ).with_lockmode('update').one()
                        
                        job.output = task.run(runner = None, config = job.config, inp = job.input)
                        # TODO: Implement CANCEL
                        task.check_output(job.output)
                        job.status = constants.JobStatus.DONE
                        model.session.commit()
                    except:
                        try:
                            raise
                        except interface.TaskValidationException:
                            log.error("The output is not valid.")
                        except model.StatementError:
                            log.error("Could not commit changes to the database.")
                        except Exception:
                            log.error("The job raised an Exception.")
                        finally:
                            model.session.rollback()
                            job.status = constants.JobStatus.FAILED
                            model.session.commit()
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
                        # implementar amb savepoint per no perdre el lock
                        model.session.rollback()
                        job.status = constants.JobStatus.SUBMIT_FAIL
                        model.session.commit()
            finally:
                model.session.rollback()
