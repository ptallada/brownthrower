#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import prettytable
import subprocess
import tempfile
import textwrap

log = logging.getLogger('brownthrower.manager')

from base import Command, error, warn, success
from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants

class ClusterCreate(Command):
    
    def __init__(self, chains, *args, **kwargs):
        super(ClusterCreate, self).__init__(*args, **kwargs)
        self._chains   = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster create <chain>
        
        Create a new cluster of the given chain.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._chains.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        chain = self._chains.get(items[0])
        if not chain:
            error("The chain '%s' is not currently available in this environment." % items[0])
            return
        
        try:
            cluster = model.Cluster(
                chain   = items[0],
                config = chain.get_config_sample(),
                status = constants.ClusterStatus.STASHED
            )
            model.session.add(cluster)
            model.session.flush()
            # Prefetch cluster.id
            cluster_id = cluster.id
            
            model.session.commit()
            success("A new cluster for chain '%s' with id %d has been created." % (items[0], cluster_id))
        
        except model.StatementError as e:
            error("The cluster could not be created.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterList(Command):
    
    def __init__(self, limit, *args, **kwargs):
        super(ClusterList, self).__init__(*args, **kwargs)
        # TODO: Warn about the limit if reached
        self._limit = limit
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster list
        
        Show a list of all the clusters registered in the database.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 0:
            return self.help(items)
        
        try:
            table = prettytable.PrettyTable(['id', 'chain', 'status', 'has config', 'has input', 'has output', '# parents', '# children'])
            table.align = 'l'
            
            clusters = model.session.query(model.Cluster).options(
                model.joinedload(model.Cluster.parents),
                model.joinedload(model.Cluster.children),
            ).limit(self._limit).all()
            for cluster in clusters:
                table.add_row([cluster.id, cluster.chain, cluster.status, cluster.config != None, cluster.input != None, cluster.output != None, len(cluster.parents), len(cluster.children)])
            
            if not clusters:
                warn("No clusters found were found.")
                return
            
            model.session.commit()
            
            print table
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterShow(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster show <id>
        
        Show detailed information about the specified cluster.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            cluster = model.session.query(model.Cluster).filter_by(id = items[0]).options(
                model.joinedload(model.Cluster.parents),
                model.joinedload(model.Cluster.children),
            ).first()
            
            if not cluster:
                warn("Could not found the cluster with id %d." % items[0])
                return
            
            table = prettytable.PrettyTable(['kind', 'id', 'chain', 'status', 'has config', 'has input', 'has output'])
            table.align = 'l'
            
            for parent in cluster.parents:
                table.add_row(['PARENT', parent.id, parent.chain, parent.status, parent.config != None, parent.input != None, parent.output != None])
            table.add_row(['#####', cluster.id, cluster.chain, cluster.status, cluster.config != None, cluster.input != None, cluster.output != None])
            for child in cluster.children:
                table.add_row(['CHILD', child.id, child.chain, child.status, child.config != None, child.input != None, child.output != None])
            
            print table
            
            model.session.commit()
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterRemove(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster remove <id>
        
        Remove the cluster with the supplied id from the stash.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            deleted = model.session.query(model.Cluster).filter_by(
                id     = items[0],
                status = constants.ClusterStatus.STASHED,
            ).delete(synchronize_session=False)
            
            model.session.commit()
            
            if deleted:
                success("The cluster has been successfully removed from the stash.")
            else: # deleted == 0
                error("The cluster could not be removed.")
        
        except BaseException as e:
            try:
                raise
            except model.IntegrityError:
                error("Some dependencies prevent this cluster from being deleted.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                model.session.rollback()
                log.debug(e)

class ClusterSubmit(Command):
    
    def __init__(self, chains, *args, **kwargs):
        super(ClusterSubmit, self).__init__(*args, **kwargs)
        self._chains   = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster submit <id>
        
        Mark the specified cluster as ready to be executed whenever there are resources available.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            cluster = model.session.query(model.Cluster).filter_by(
                id     = items[0],
                status = constants.ClusterStatus.STASHED,
            ).with_lockmode('update').first()
            
            if not cluster:
                error("The cluster could not be submitted.")
                return
            
            chain = self._chains.get(cluster.chain)
            if not chain:
                error("The chain '%s' is not currently available in this environment." % cluster.chain)
                return
            
            chain.validate_config(cluster.config)
            if not cluster.parents:
                chain.validate_input(cluster.input)
            
            # TODO: Shall reset all the other fields
            cluster.status = constants.ClusterStatus.READY
            model.session.commit()
            
            success("The cluster has been successfully marked as ready for execution.")
        
        except BaseException as e:
            try:
                raise
            except interface.ChainValidationException:
                error("The cluster has an invalid config or input.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                model.session.rollback()
                log.debug(e)

class ClusterReset(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster reset <id>
        
        Return the specified cluster to the stash.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            resetted = model.session.query(model.Cluster).filter(
                model.Cluster.id == items[0],
                model.Cluster.status.in_([
                    constants.ClusterStatus.READY,
                    constants.ClusterStatus.PROLOG_FAIL,
                ])
            ).update(
                #TODO: Shall reset all the other fields
                {'status' : constants.ClusterStatus.STASHED},
                synchronize_session = False \
            )
            model.session.commit()
            
            if resetted:
                success("The cluster has been successfully returned to the stash.")
            else: # resetted == 0
                error("The cluster could not be returned to the stash.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterLink(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster link <parent_id> <child_id>
        
        Establish a dependency between two clusters.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            parent = model.session.query(model.Cluster).filter_by(
                id = items[0],
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Cluster).filter_by(
                id     = items[1],
                status = constants.ClusterStatus.STASHED
            ).with_lockmode('read').first()
            
            if not (parent and child):
                warn("It is not possible to establish a parent-child dependency between these clusters.")
                return
            
            dependency = model.ClusterDependency(
                child_cluster_id  = child.id,
                parent_cluster_id = parent.id
            )
            model.session.add(dependency)
            model.session.commit()
            
            success("The parent-child dependency has been successfully established.")
            
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterUnlink(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster unlink <parent_id> <child_id>
        
        Remove the dependency between the specified clusters.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            parent = model.session.query(model.Cluster).filter_by(
                id = items[0],
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Cluster).filter_by(
                id     = items[1],
                status = constants.ClusterStatus.STASHED,
            ).with_lockmode('read').first()
            
            if not (parent and child):
                warn("It is not possible to remove the parent-child dependency.")
                return
            
            deleted = model.session.query(model.ClusterDependency).filter_by(
                parent_cluster_id = parent.id,
                child_cluster_id  = child.id
            ).delete(synchronize_session=False)
            model.session.commit()
            
            if not deleted:
                error("Could not remove the parent-child dependency.")
            else:
                success("The parent-child dependency has been successfully removed.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterCancel(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster cancel <id>
        
        Cancel the specified cluster as soon as possible.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            cluster = model.session.query(model.Cluster).filter(
                model.Cluster.id == items[0],
                model.Cluster.status.in_([
                    constants.ClusterStatus.DEPLOYED,
                    constants.ClusterStatus.PROCESSING,
                    constants.ClusterStatus.FAILING,
                ])
            ).with_lockmode('update').first()
            
            if not cluster:
                error("The cluster could not be cancelled.")
                return
            
            stashed = model.session.query(model.Job).filter_by(
                cluster = cluster,
                status  = constants.JobStatus.READY
            ).update(
            )
            with_lockmode('update').all()
            
            
            
            
            
            
            
            
            model.session.query(model.Job).filter_by(
                cluster = cluster,
                status  = constants.JobStatus.READY,
            ).update(
                #TODO: Shall reset all the other fields
                {'status' : constants.JobStatus.STASHED},
                synchronize_session = False \
            )
            
            model.session.query(model.Job).filter(
                model.Job.cluster == cluster,
                model.Job.status.in_([
                    constants.JobStatus.QUEUED,
                    constants.JobStatus.RUNNING,
                ])
            ).update(
                #TODO: Shall reset all the other fields
                {'status' : constants.JobStatus.CANCELLING},
                synchronize_session = False \
            ) 
            
            # Fer la matriu de transicions
            
            model.session.commit()
            
            if cancel:
                success("The cluster has been marked to be cancelled as soon as possible.")
            else: # cancel == 0
                error("The cluster could not be marked to be cancelled.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class ClusterEdit(Command):
    
    _dataset_attr = {
        'config' : {
            'field'    : model.Cluster.config,
            'sample'   : lambda chain: chain.get_config_sample,
            'validate' : lambda chain: chain.validate_config,
        },
        'input'  : {
            'field'    : model.Cluster.input,
            'sample'   : lambda chain: chain.get_input_sample,
            'validate' : lambda chain: chain.validate_input,
        }
    }
    
    def __init__(self, chains, editor, *args, **kwargs):
        super(ClusterEdit, self).__init__(*args, **kwargs)
        self._chains = chains
        self._editor = editor
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster edit <dataset> <id>
        
        Edit the specified dataset of a cluster.
        Valid values for the dataset parameter are: 'input' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [attr
                        for attr in self._dataset_attr.keys()
                        if attr.startswith(text)]
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_attr)
        ):
            return self.help(items)
        
        try:
            cluster = model.session.query(model.Cluster).filter_by(
                id     = items[1],
                status = constants.ClusterStatus.STASHED,
            ).with_lockmode('update').first()
            
            if not cluster:
                warn("Could not find or lock the cluster for editing.")
                return
            
            chain = self._chains.get(cluster.chain)
            if not chain:
                error("The chain '%s' is not currently available in this environment." % cluster.chain)
                return
            
            field    = self._dataset_attr[items[0]]['field']
            sample   = self._dataset_attr[items[0]]['sample'](chain)()
            validate = self._dataset_attr[items[0]]['validate'](chain)
            
            current_value = getattr(cluster, field.key)
            if not current_value:
                current_value = sample
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(current_value)
                fh.flush()
                
                subprocess.check_call([self._editor, fh.name])
                
                fh.seek(0)
                new_value = fh.read()
            
            validate(new_value)
            
            setattr(cluster, field.key, new_value)
            model.session.commit()
            
            success("The cluster dataset has been successfully modified.")
        
        except BaseException as e:
            try:
                raise
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except interface.ChainValidationException:
                error("The new value for the %s is not valid." % items[0])
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
                model.session.rollback()

class ClusterOutput(Command):
    
    def __init__(self, viewer, *args, **kwargs):
        super(ClusterOutput, self).__init__(*args, **kwargs)
        self._viewer = viewer
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: cluster output <id>
        
        Show the output of a completed cluster.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            cluster = model.session.query(model.Cluster).filter_by(
                id     = items[0],
                status = constants.ClusterStatus.DONE,
            ).first()
            
            if not cluster:
                warn("The output from cluster %d cannot be shown." % items[0])
                return
            
            cluster_output = cluster.output
            
            model.session.commit()
            
            viewer = subprocess.Popen([self._viewer], stdin=subprocess.PIPE)
            viewer.communicate(input=cluster_output)
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()
