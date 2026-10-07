# req: END-007, END-008, END-010
"""Same-PID launcher for a frozen conservative process address-space ceiling.

This is a kernel-enforced upper bound, never an allocator measurement. The
owned, unprivileged campaign programs must retain the inherited hard limit;
complete incarnation/descendant accounting remains the controller's job.
"""
import json
import os
from pathlib import Path
import resource
import select
import sys
import time

if __name__ != "__main__":
    from .ticketdesk_lifecycle_resources import MAX_U64, ProcessMemory, ResourceError, require

MIN_LIMIT = 64 * 1024**2
MAX_LIMIT = 32 * 1024**3
MAX_PROOF = 1024
BASIS = 'linux_hard_rlimit_as_upper_bound'


class AddressSpaceLaunch:
    def __init__(self, arguments, limit):
        require(type(limit) is int and MIN_LIMIT <= limit <= MAX_LIMIT,
                'process_address_space_limit')
        require(type(arguments) in (list, tuple) and 0 < len(arguments) <= 64
                and all(type(value) is str and '\0' not in value for value in arguments)
                and sum(len(value.encode()) for value in arguments) <= 65536,
                'process_address_space_arguments')
        self.limit = limit
        hard = resource.getrlimit(resource.RLIMIT_AS)[1]
        self.parent_hard = None if hard == resource.RLIM_INFINITY else hard
        if self.parent_hard is not None:
            require(type(hard) is int and 0 <= hard <= MAX_U64, 'process_address_space_proof')
            # Linux permits lowering a hard limit below existing mappings.
            # Such mappings survive fork, so include the actual parent image
            # as well as every further mapping its finite hard limit permits.
            self.birth_bound = max(hard, ProcessMemory(os.getpid()).sample()['allocator_bytes'])
        else:
            self.birth_bound = None
        self.read, self.write = os.pipe2(os.O_CLOEXEC)
        self.arguments = [sys.executable, '-I', str(Path(__file__).resolve()),
                          str(limit), str(self.write), *arguments]

    def __enter__(self): return self

    def __exit__(self, *_):
        for name in ('read', 'write'):
            descriptor = getattr(self, name)
            if descriptor is not None:
                os.close(descriptor); setattr(self, name, None)

    def options(self, options):
        value = dict(options)
        inherited = value.get('pass_fds', ())
        require(type(inherited) in (tuple, list), 'process_address_space_descriptors')
        value['pass_fds'] = (*inherited, self.write)
        return value

    def confirm(self):
        os.close(self.write); self.write = None
        deadline = time.monotonic() + 10
        encoded = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0 and select.select([self.read], [], [], remaining)[0],
                    'process_address_space_proof')
            block = os.read(self.read, MAX_PROOF + 1 - len(encoded))
            if not block: break
            encoded.extend(block)
            require(len(encoded) <= MAX_PROOF, 'process_address_space_proof')
        try:
            value = json.loads(encoded)
        except (ValueError, UnicodeError) as error:
            raise ResourceError('process_address_space_proof') from error
        require(type(value) is dict and value.keys() == {'enforced', 'limit_bytes',
                    'launch_virtual_peak_bytes', 'inherited_hard_limit_bytes'} and value['enforced'] is True
                and type(value['limit_bytes']) is int and value['limit_bytes'] == self.limit
                and type(value['launch_virtual_peak_bytes']) is int
                and 0 < value['launch_virtual_peak_bytes'] <= self.limit,
                'process_address_space_proof')
        inherited = value['inherited_hard_limit_bytes']
        require(inherited is None or type(inherited) is int and self.limit <= inherited <= MAX_U64,
                'process_address_space_proof')
        if self.parent_hard is not None:
            require(inherited is not None and inherited <= self.parent_hard, 'process_address_space_proof')
        value['lifetime_upper_bound_bytes'] = self.birth_bound
        return value


def launch():
    # Running this file directly intentionally imports no repository modules:
    # the launcher works before the worker's application environment is loaded.
    descriptor = None
    try:
        if not 4 <= len(sys.argv) <= 67: raise ValueError()
        limit, descriptor = int(sys.argv[1]), int(sys.argv[2])
        if not MIN_LIMIT <= limit <= MAX_LIMIT or descriptor < 3: raise ValueError()
        os.set_inheritable(descriptor, False)
        if os.geteuid() == 0: raise ValueError()
        with open('/proc/self/status', 'rb') as stream:
            encoded = stream.read(65537)
        if len(encoded) > 65536: raise ValueError()
        selected = {}
        for line in encoded.splitlines():
            key, _, value = line.partition(b':')
            if key in (b'VmPeak', b'CapEff'):
                if key in selected: raise ValueError()
                selected[key] = value.split()
        if selected.keys() != {b'VmPeak', b'CapEff'}: raise ValueError()
        if selected[b'VmPeak'][1:] != [b'kB'] or len(selected[b'CapEff']) != 1:
            raise ValueError()
        peak = int(selected[b'VmPeak'][0]) * 1024
        if not 0 < peak <= limit or int(selected[b'CapEff'][0], 16) & (1 << 24):
            raise ValueError()
        inherited = resource.getrlimit(resource.RLIMIT_AS)[1]
        inherited = None if inherited == resource.RLIM_INFINITY else inherited
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
        if resource.getrlimit(resource.RLIMIT_AS) != (limit, limit): raise ValueError()
        # Lowering RLIMIT_AS below an existing mapping is allowed by Linux.
        # Recheck the kernel lifetime high water after enforcement so no
        # pre-enforcement launcher allocation can evade this bound.
        with open('/proc/self/status', 'rb') as stream:
            after = stream.read(65537)
        peaks = [line.partition(b':')[2].split() for line in after.splitlines()
                 if line.startswith(b'VmPeak:')]
        if len(after) > 65536 or len(peaks) != 1 or peaks[0][1:] != [b'kB']:
            raise ValueError()
        peak = int(peaks[0][0]) * 1024
        if not 0 < peak <= limit: raise ValueError()
        proof = json.dumps(dict(enforced=True, limit_bytes=limit,
                                launch_virtual_peak_bytes=peak,
                                inherited_hard_limit_bytes=inherited), sort_keys=True).encode()
        if os.write(descriptor, proof) != len(proof): raise ValueError()
        os.close(descriptor); descriptor = None
        os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
    except (OSError, ValueError, IndexError, MemoryError):
        if descriptor is not None: os.close(descriptor)
        os.write(2, b'process_address_space_launch_refused\n')
        return 127


if __name__ == '__main__':
    sys.exit(launch())
