#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from base import Command, error, warn

class ChainList(Command):
    
    def __init__(self, chains, *args, **kwargs):
        super(ChainList, self).__init__(*args, **kwargs)
        self._chains = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: chain list
        
        Show a list of all the chains available in this environment.
        """)
    
    def complete(self, text, items):
        pass
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(self._chains) == 0:
            warn("There are no chains currently registered in this environment.")
            return
        
        table = prettytable.PrettyTable(['name', 'description'], sortby='name')
        table.align = 'l'
        for name, chain in self._chains.iteritems():
            table.add_row([name, chain.get_help()[0]])
        
        print table

class ChainShow(Command):
    
    def __init__(self, chains, *args, **kwargs):
        super(ChainShow, self).__init__(*args, **kwargs)
        self._chains = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: chain show <name>
        
        Show a detailed description of the specified chain.
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
        if chain:
            desc = chain.get_help()
            print desc[0]
            print desc[1]
        else:
            error("The chain '%s' is not currently available in this environment." % items[0])

class ChainSchema(Command):
    
    _dataset_fn = {
        'config' : lambda chain: chain.get_config_schema,
        'input'  : lambda chain: chain.get_input_schema,
        'output' : lambda chain: chain.get_output_schema,
    }
    
    def __init__(self, chains, *args, **kwargs):
        super(ChainSchema, self).__init__(*args, **kwargs)
        self._chains = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: chain schema <dataset> <chain>
        
        Show the schema of the specified dataset for a chain.
        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [value
                        for value in self._dataset_fn.keys()
                        if value.startswith(text)]
            return matching
        
        if (len(items) == 1) and (items[0] in self._dataset_fn):
            matching = [key
                        for key in self._chains.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn) or
            (items[1] not in self._chains.keys())
        ):
            return self.help(items)
        
        chain = self._chains.get(items[1])
        if not chain:
            error("The chain '%s' is not currently available in this environment." % items[1])
            return
        
        print self._dataset_fn[items[0]](chain)()

class ChainSample(Command):
    
    _dataset_fn = {
        'config' : lambda chain: chain.get_config_sample,
        'input'  : lambda chain: chain.get_input_sample,
        'output' : lambda chain: chain.get_output_sample,
    }
        
    def __init__(self, chains, *args, **kwargs):
        super(ChainSample, self).__init__(*args, **kwargs)
        self._chains = chains
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: chain sample <dataset> <chain>
        
        Show a sample of the specified dataset for a chain.
        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [value
                        for value in self._dataset_fn
                        if value.startswith(text)]
            return matching
        
        if (len(items) == 1) and (items[0] in self._dataset_fn):
            matching = [key
                        for key in self._chains.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn.keys()) or
            (items[1] not in self._chains.keys())
        ):
            return self.help(items)
        
        chain = self._chains.get(items[1])
        if not chain:
            error("The chain '%s' is not currently available in this environment." % items[1])
            return
        
        print self._dataset_fn[items[0]](chain)()
