
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import subprocess

import radical.utils as ru

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class PBSPro(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):
        # TODO: $NCPUS?!?! = 1 on archer

        pbspro_nodefile = os.environ.get('PBS_NODEFILE')
        if pbspro_nodefile is None:
            raise RuntimeError('$PBS_NODEFILE not set')
        self._log.info('PBS_NODEFILE: %s', pbspro_nodefile)

        # Dont need to parse the content of nodefile for PBSPRO, only the length
        # is interesting, as there are only duplicate entries in it.
        pbspro_nodes        = [line.strip() for line in open(pbspro_nodefile)]
        pbspro_nodes_length = len(pbspro_nodes)

        # Number of Processors per Node
        val = os.environ.get('NUM_PPN') or os.environ.get('SAGA_PPN')
        if not val:
            raise RuntimeError('$NUM_PPN and $SAGA_PPN not set!')

        pbspro_num_ppn = int(val)

        # Number of Nodes allocated
        val = os.environ.get('NODE_COUNT')
        if val:
            pbspro_node_count = int(val)
        else:
            pbspro_node_count = len(set(pbspro_nodes))
            self._log.warn('$NODE_COUNT not set - use %d', pbspro_node_count)

        # Number of Parallel Environments
        val = os.environ.get('NUM_PES')
        if val:
            pbspro_num_pes = int(val)
        else:
            pbspro_num_pes = len(pbspro_nodes)
            self._log.warn('$NUM_PES not set - use %d', pbspro_num_pes)

        try:
            pbspro_vnodes = self._parse_pbspro_vnodes()
        except:
            self._log.exception('node parsing failed')
            raise

        # Verify that $NUM_PES == $NODE_COUNT * $NUM_PPN == len($PBS_NODEFILE)
        if not (pbspro_node_count * pbspro_num_ppn
                == pbspro_num_pes
                == pbspro_nodes_length):
            self._log.warning(
                'NUM_PES != NODE_COUNT * NUM_PPN != len($PBS_NODEFILE)')

        info.cores_per_node = pbspro_num_ppn

        # node names are unique, so can serve as node uids
        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(pbspro_vnodes))]

        return info

    # --------------------------------------------------------------------------
    #
    def _parse_pbspro_vnodes(self):

        # PBS Job ID
        pbspro_jobid = os.environ.get('PBS_JOBID')
        if not pbspro_jobid:
            raise RuntimeError('$PBS_JOBID not set')

        # Get the output of qstat -f for this job
        output = subprocess.check_output(['qstat', '-f', pbspro_jobid])

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            line = ru.as_string(line)
            # Detect start of entry
            if 'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if ' = ' not in line:
                    vnodes_str += line.strip()
                else:
                    break

        # Get the RHS of the entry
        rhs = vnodes_str.split('=', 1)[1].strip()
        self._log.debug('input: %s', rhs)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = rhs.find(')+(')

            node_str = rhs[1:idx]
            nodes_list.append(node_str)
            rhs = rhs[idx + 2:]

            if idx < 0:
                break

        vnodes_list = []
        cpus_list = []
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for _slice in slices:
                vnode, cpus = _slice.split(':')
                cpus = int(cpus.split('=')[1])
                self._log.debug('vnode: %s cpus: %s', vnode, cpus)
                vnodes_list.append(vnode)
                cpus_list.append(cpus)

        self._log.debug('vnodes: %s', vnodes_list)
        self._log.debug('cpus: %s', cpus_list)

        cpus_list = list(set(cpus_list))
        min_cpus = int(min(cpus_list))

        if len(cpus_list) > 1:
            self._log.debug('Detected vnodes of different sizes: %s, ' +
                            'the minimal is: %d.', cpus_list, min_cpus)

        node_list = []
        for vnode in vnodes_list:
            node_list.append(vnode)

        # only unique node names
        node_list = list(set(node_list))
        self._log.debug('Node list: %s', node_list)

        # Return the list of node names
        return sorted(node_list)

# ------------------------------------------------------------------------------

