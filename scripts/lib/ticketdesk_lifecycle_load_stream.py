# req: END-007, END-008, END-010
"""Closed day-sized load observations; preparation and recovery keep their limits."""
import json

MAX_LINE = 4096
MAX_U64 = 2**64 - 1
MAX_WINDOW_COMPLETIONS = 200000
READY = frozenset({'schema','workers','queue_slots'})
SHARED_READY = READY | {'origin_monotonic_ns'}
WINDOW = frozenset({'schema','start_s','end_s','offered_commands','offered_reads',
    'completed_commands','completed_reads','settled_submission_attempts','transport_attempts',
    'retries','p95_us','p99_us','load_peak_in_flight','reserved_auxiliary_slots','latency_overflow'})
STOPPED = frozenset({'schema','completed_updates','completed_reads','post_duration_updates',
                     'post_duration_reads'})


class LoadStreamError(ValueError):
    """Closed codes contain no worker output or process identity."""


def require(condition, code):
    if not condition: raise LoadStreamError(code)


def number(value):
    require(type(value) is int and 0 <= value <= MAX_U64,'load_stream_number')
    return value


def parse_integer(value):
    require(len(value) <= 20,'load_stream_number')
    return number(int(value))


def reject_number(_): raise LoadStreamError('load_stream_number')


def unique_object(pairs):
    value={}
    for key,item in pairs:
        require(key not in value,'load_stream_json')
        value[key]=item
    return value


class CampaignLoadStream:
    def __init__(self, *, duration_s, require_shared_clock=False):
        require(type(require_shared_clock) is bool,'load_stream_clock')
        self.require_shared_clock=require_shared_clock
        self.origin_ns=None
        number(duration_s)
        require(duration_s % 300 == 0 and 73 <= duration_s//300 <= 288,'load_stream_duration')
        self.windows=duration_s//300
        self._ready=False
        self._terminal=False
        self._eof=False
        self._failed=False
        self._cursor=0
        self._bytes=0
        self._commands=0
        self._reads=0

    @property
    def complete(self): return self._eof and self._terminal and not self._failed

    def read(self, source):
        require(not self._failed,'load_stream_failed')
        try:
            line=source.readline(MAX_LINE+1)
            if not line:
                require(self._terminal,'load_stream_incomplete')
                self._eof=True
                return None
            require(not self._eof and not self._terminal,'load_stream_trailing')
            self._bytes+=len(line)
            require(type(line) is bytes and len(line) <= MAX_LINE and line.endswith(b'\n')
                    and self._bytes <= (self.windows+2)*MAX_LINE,'load_stream_bound')
            value=json.loads(line,object_pairs_hook=unique_object,parse_int=parse_integer,
                             parse_float=reject_number,parse_constant=reject_number)
            require(type(value) is dict and type(value.get('schema')) is str,'load_stream_shape')
            for key,item in value.items():
                if key!='schema': number(item)
            if not self._ready:
                if self.require_shared_clock:
                    require(value.keys()==SHARED_READY
                            and value['schema']=='riffdb.ticketdesk-lifecycle-load-ready/v2'
                            and value['origin_monotonic_ns']>0,'load_stream_clock')
                    self.origin_ns=value['origin_monotonic_ns']
                else:
                    require(value.keys()==READY
                            and value['schema']=='riffdb.ticketdesk-lifecycle-load-ready/v1','load_stream_ready')
                require(value['workers']==28 and value['queue_slots']==32768,'load_stream_ready')
                self._ready=True
            elif self._cursor < self.windows:
                require(value.keys()==WINDOW and value['schema']=='riffdb.ticketdesk-lifecycle-load-probe/v1',
                        'load_stream_window')
                require(value['start_s']==self._cursor*300 and value['end_s']==(self._cursor+1)*300
                        and value['offered_commands']==30000 and value['offered_reads']==120000,
                        'load_stream_window')
                completed=value['completed_commands']+value['completed_reads']
                require(completed <= MAX_WINDOW_COMPLETIONS and value['load_peak_in_flight']<=28
                        and value['reserved_auxiliary_slots']==4 and value['latency_overflow']<=completed
                        and value['p95_us']<=value['p99_us'],'load_stream_accounting')
                require(value['transport_attempts']==value['settled_submission_attempts']==completed+value['retries']
                        and value['transport_attempts']<=MAX_WINDOW_COMPLETIONS*240,'load_stream_accounting')
                self._commands+=value['completed_commands'];self._reads+=value['completed_reads']
                self._cursor+=1
            else:
                require(value.keys()==STOPPED and value['schema']=='riffdb.ticketdesk-lifecycle-load-stopped/v1',
                        'load_stream_terminal')
                require(value['completed_updates']==self._commands+value['post_duration_updates']==self.windows*30000
                        and value['completed_reads']==self._reads+value['post_duration_reads']==self.windows*120000,
                        'load_stream_terminal')
                self._terminal=True
            return value
        except (ValueError, UnicodeError, RecursionError, OSError, TypeError) as error:
            self._failed=True
            if isinstance(error,LoadStreamError): raise
            raise LoadStreamError('load_stream_json') from error
