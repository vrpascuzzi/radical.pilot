
# pylint: disable=protected-access, unused-argument


from radical.pilot.agent.launch_method.base import LaunchMethod

import radical.utils as ru
import pytest
import warnings

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
# @mock.patch.object(LaunchMethod, '_configure', return_value=None)
# def test_init(mocked_configure):
#     session = mock.Mock()
#     session._log = mock.Mock()
#     lm = LaunchMethod(name='test', cfg={}, session=session)
#     assert lm.name     == 'test'
#     assert lm._cfg     == {}
#     assert lm._session == session
#     assert lm._log     == session._log


# ------------------------------------------------------------------------------
#
def test_configure():

    session = mock.Mock()
    session._log = mock.Mock()
    with pytest.raises(NotImplementedError):
        LaunchMethod(name='test', cfg={}, session=session)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(LaunchMethod,'__init__',return_value=None)
def test_get_mpi_info(mocked_init):

    lm = LaunchMethod(name=None, cfg={}, session=None)
    lm._log = mock.Mock()
    ru.sh_callout = mock.Mock()
    ru.sh_callout.side_effect = [['test',1,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    if version is None:
        assert True
    else:
        assert False
    assert flavor == 'unknown'

    ru.sh_callout.side_effect = [['test',1,1],['mpirun (Open MPI) 2.1.2\n\n\
                                  Report bugs to http://www.open-mpi.org/community/help/\n',3,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    assert version == '2.1.2'
    assert flavor == 'OMPI'

    ru.sh_callout.side_effect = [['test',1,1],['HYDRA build details:',3,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    assert version == ''
    assert flavor == 'HYDRA'

    ru.sh_callout.side_effect = [['test',1,1],['Intel(R) MPI Library for Linux* OS,\n\n\
                                               Version 2019 Update 5 Build 20190806\n\n\
                                               Copyright 2003-2019, Intel Corporation.',3,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    assert version == ''
    assert flavor == 'HYDRA'

    ru.sh_callout.side_effect = [['test',1,1],['MVAPICH2 2.3b',3,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    try: 
        assert version == '3.2b'
        assert flavor == 'MVAPICH2'
    except:
        warnings.warn(UserWarning("MVAPICH MPI flavor Not implemented yet"))
