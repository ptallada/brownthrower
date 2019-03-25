import os
import subprocess
import sys

from contextlib import contextmanager

@contextmanager
def clone_stdout_stderr(stdout_fname, stderr_fname):
    sys.stdout.flush()
    sys.stderr.flush()
    
    # Disable buffering 
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w')
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w')
    
    tee_stdout = subprocess.Popen(['tee', stdout_fname], stdin=subprocess.PIPE)
    tee_stderr = subprocess.Popen(['tee', stderr_fname], stdin=subprocess.PIPE, stdout=sys.stderr)
    
    with os.fdopen(os.dup(sys.stdout.fileno()), 'w') as stdout:
        with os.fdopen(os.dup(sys.stderr.fileno()), 'w') as stderr:
            os.dup2(tee_stdout.stdin.fileno(), sys.stdout.fileno())
            os.dup2(tee_stderr.stdin.fileno(), sys.stderr.fileno())
            try:
                yield
            finally:
                sys.stdout.flush()
                sys.stderr.flush()
                os.dup2(stdout.fileno(), sys.stdout.fileno())
                os.dup2(stderr.fileno(), sys.stderr.fileno())
                tee_stdout.terminate()
                tee_stderr.terminate()
                tee_stdout.wait()
                tee_stderr.wait()
