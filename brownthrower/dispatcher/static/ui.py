#! /usr/bin/env python
# -*- coding: utf-8 -*-

import urwid

class MainScreen(object):
    def __init__(self, runner_path, runner_args, ce_queue, allowed_tasks):
        self._create_widgets(runner_path, runner_args, ce_queue, allowed_tasks)
        self._palette = [
            ('header',  'black',     'dark cyan', 'standout'),
            ('label',   'light red', 'default',   'standout'),
            ('value',   'default',   'default',   'default'),
        ]
        self._screen = urwid.raw_display.Screen()
        self._loop = urwid.MainLoop(
            self._top,
            self._palette,
            self._screen,
            unhandled_input=self._unhandled,
        )
    
    def _unhandled(self, key):
        if key == 'esc':
            raise urwid.ExitMainLoop()
    
    def set_callback(self, fd, callback):
        self._loop.event_loop.watch_file(fd, callback)
    
    def _create_widgets(self, runner_path, runner_args, ce_queue, allowed_tasks):
        tasks = '*'
        if allowed_tasks:
            tasks = ' ,'.join(allowed_tasks)
        
        self._header = urwid.AttrWrap(
            urwid.Text("Brownthrower dispatcher static", align='center'),
            'header',
        )
        
        self._runner_path_label = urwid.Text(('label', "Remote runner path:"))
        self._runner_args_label = urwid.Text(('label', "Remote runner arguments:"))
        self._ce_queue_label    = urwid.Text(('label', "gLite CE endpoint:"))
        self._tasks_label       = urwid.Text(('label', "Eligible tasks:"))
        
        self._runner_path_value = urwid.Text(('value', runner_path))
        self._runner_args_value = urwid.Text(('value', runner_args))
        self._ce_queue_value    = urwid.Text(('value', ce_queue))
        self._tasks_value       = urwid.Text(('value', tasks))
        
        self._runner_path = urwid.Columns([
            ('fixed', 24, self._runner_path_label),
            self._runner_path_value,
        ], 2)
        self._runner_args = urwid.Columns([
            ('fixed', 24, self._runner_args_label),
            self._runner_args_value,
        ], 2)
        self._ce_queue = urwid.Columns([
            ('fixed', 24, self._ce_queue_label),
            self._ce_queue_value,
        ], 2)
        self._tasks = urwid.Columns([
            ('fixed', 24, self._tasks_label),
            self._tasks_value,
        ], 2)
        
        self._bt_status = urwid.Pile([])
        
        self._glite_status = urwid.Pile([])
        
        self._status = urwid.Columns([
                ('fixed', 21, urwid.LineBox(urwid.Padding(self._bt_status,    left=1, right=1), title='BT status')),
                ('fixed', 25, urwid.LineBox(urwid.Padding(self._glite_status, left=1, right=1), title='gLite status')),
            ], 2)
        
        divider = urwid.Divider()
        
        self._lw = urwid.SimpleListWalker([
            divider,
            self._runner_path,
            self._runner_args,
            self._ce_queue,
            self._tasks,
            divider,
            self._status,
        ])
        self._body = urwid.Padding(
            urwid.ListBox(self._lw),
            left=2, right=2,
        )
        
        self._top = urwid.Frame(self._body, self._header)
    
    def run(self):
        try:
            self._loop.run()
        except urwid.ExitMainLoop:
            pass
    
    def _create_bt_row(self, label, value):
        return urwid.Columns([
            ('fixed', 10, urwid.Text(('label', label))),
            ('fixed',  5, urwid.Text(('value', str(value)), align='right')),
        ], 2)
    
    def _create_glite_row(self, label, value):
        return urwid.Columns([
            ('fixed', 14, urwid.Text(('label', label))),
            ('fixed',  5, urwid.Text(('value', str(value)), align='right')),
        ], 2)
    
    def update_bt(self, status):
        self._bt_status.contents[:] = []
        for label in sorted(status.iterkeys()):
            row = self._create_bt_row(label, status[label])
            self._bt_status.contents.append((row, ('pack', None)))
    
    def update_glite(self, status):
        self._glite_status.contents[:] = []
        for label in sorted(status.iterkeys()):
            row = self._create_glite_row(label, status[label])
            self._glite_status.contents.append((row, ('pack', None)))
