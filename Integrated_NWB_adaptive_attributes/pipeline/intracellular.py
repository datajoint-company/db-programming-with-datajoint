import os
import pathlib
import numpy as np
import datajoint as dj
import h5py as h5
import pynwb

from . import experiment, utilities
from .nwb_adapter import *

schema = dj.schema(dj.config['custom'].get('database.prefix', '') + 'intracellular')

sess_data_dir = pathlib.Path(dj.config['custom'].get('intracellular_directory')).as_posix()


@schema
class WholeCellDevice(dj.Lookup):
    definition = """ # Description of the device used for electrical stimulation
    device_name: varchar(32)
    ---
    device_desc = "": varchar(1024)
    """


@schema
class BrainLocation(dj.Manual):
    definition = """
    brain_region: varchar(32)
    hemisphere: enum('left', 'right')
    ---
    coordinate_ref: enum('bregma', 'lambda')
    coordinate_ap: decimal(4,2)    # in mm, anterior positive, posterior negative 
    coordinate_ml: decimal(4,2)    # in mm, always postive, number larger when more lateral
    coordinate_dv: decimal(4,2)    # in mm, always postive, number larger when more ventral (deeper)
    """


@schema
class Cell(dj.Manual):
    definition = """ # A cell undergone intracellular recording in this session
    -> experiment.Session
    cell_id: varchar(36) # a string identifying the cell in which this intracellular recording is concerning
    ---
    cell_type: enum('excitatory','inhibitory','N/A')
    -> BrainLocation
    -> WholeCellDevice
    """


@schema
class MembranePotential(dj.Imported):
    definition = """
    -> Cell
    ---
    nwb_patch_clamp: <patch_clamp_series>
    membrane_potential: longblob    # (mV) membrane potential recording at this cell
    membrane_potential_wo_spike: longblob # (mV) membrane potential without spike data, derived from membrane potential recording    
    membrane_potential_start_time: float # (s) first timepoint of membrane potential recording
    membrane_potential_sampling_rate: float # (Hz) sampling rate of membrane potential recording
    """

    def make(self, key):
        # ============ Dataset ============
        # Get the Session definition from the keys of this session
        animal_id = key['subject_id']
        date_of_experiment = key['session_time']
        # Search the files in filenames to find a match for "this" session (based on key)
        sess_data_file = utilities.find_session_matched_nwbfile(sess_data_dir, animal_id, date_of_experiment)
        if sess_data_file is None:
            raise FileNotFoundError(f'IntracellularAcquisition import failed for: {animal_id} - {date_of_experiment}')
        nwb = h5.File(os.path.join(sess_data_dir, sess_data_file), 'r')
        #  ============= Now read the data and start ingesting =============
        print('Insert intracellular data for: subject: {0} - date: {1} - cell: {2}'.format(key['subject_id'],
                                                                                           key['session_time'],
                                                                                           key['cell_id']))

        mp = nwb['acquisition']['timeseries']['membrane_potential']['data'][()]
        mp_wo_spike = nwb['analysis']['Vm_wo_spikes']['membrane_potential_wo_spike']['data'][()]
        mp_time_stamps = nwb['acquisition']['timeseries']['membrane_potential']['timestamps'][()]
        mp_start_time = mp_time_stamps[0]
        mp_fs = 1 / np.mean(np.diff(mp_time_stamps))

        cell = (Cell & key).fetch1()

        # -- pynwb.icephys.PatchClampSeries --
        sess_nwb = (experiment.Session & key).fetch1('nwb_file')
        mp_nwb = sess_nwb.copy()
        mp_nwb.io = sess_nwb.io

        whole_cell_device = mp_nwb.create_device(name=cell['device_name'])
        ic_electrode = mp_nwb.create_ic_electrode(
            name=cell['cell_id'],
            device=whole_cell_device,
            description='N/A',
            filtering='low-pass: 10kHz',
            location='; '.join([f'{k}: {str(v)}'
                                for k, v in (BrainLocation & cell).fetch1().items()]))
        patch_clamp = pynwb.icephys.PatchClampSeries(name='membrane_potential',
                                                     electrode=ic_electrode, unit='mV',
                                                     conversion=1e-3, gain=1.0,
                                                     data=mp, starting_time=mp_start_time, rate=mp_fs)

        mp_nwb.add_acquisition(patch_clamp)

        # -- MembranePotential
        self.insert1(dict(key,
                          nwb_patch_clamp=mp_nwb,
                          membrane_potential=mp,
                          membrane_potential_wo_spike=mp_wo_spike,
                          membrane_potential_start_time=mp_start_time,
                          membrane_potential_sampling_rate=mp_fs))
        nwb.close()


@schema
class CurrentInjection(dj.Imported):
    definition = """
    -> Cell
    ---
    nwb_current_stim: <current_stim_series>
    current_injection: longblob
    current_injection_start_time: float  # first timepoint of current injection recording
    current_injection_sampling_rate: float  # (Hz) sampling rate of current injection recording
    """

    def make(self, key):
        # ============ Dataset ============
        # Get the Session definition from the keys of this session
        animal_id = key['subject_id']
        date_of_experiment = key['session_time']
        # Search the files in filenames to find a match for "this" session (based on key)
        sess_data_file = utilities.find_session_matched_nwbfile(sess_data_dir, animal_id, date_of_experiment)
        if sess_data_file is None:
            raise FileNotFoundError(f'IntracellularAcquisition import failed for: {animal_id} - {date_of_experiment}')
        nwb = h5.File(os.path.join(sess_data_dir, sess_data_file), 'r')
        #  ============= Now read the data and start ingesting =============
        print('Insert intracellular data for: subject: {0} - date: {1} - cell: {2}'.format(key['subject_id'],
                                                                                           key['session_time'],
                                                                                           key['cell_id']))

        ci = nwb['acquisition']['timeseries']['current_injection']['data'][()]
        ci_time_stamps = nwb['acquisition']['timeseries']['current_injection']['timestamps'][()]
        ci_start_time = ci_time_stamps[0]
        ci_fs = 1 / np.mean(np.diff(ci_time_stamps))

        cell = (Cell & key).fetch1()

        # -- pynwb.icephys.CurrentClampStimulusSeries --
        sess_nwb = (experiment.Session & key).fetch1('nwb_file')
        ci_nwb = sess_nwb.copy()
        ci_nwb.io = sess_nwb.io

        whole_cell_device = ci_nwb.create_device(name=cell['device_name'])
        ic_electrode = ci_nwb.create_ic_electrode(
            name=cell['cell_id'],
            device=whole_cell_device,
            description='N/A',
            filtering='low-pass: 10kHz',
            location='; '.join([f'{k}: {str(v)}'
                                for k, v in (BrainLocation & cell).fetch1().items()]))

        current_stim = pynwb.icephys.CurrentClampStimulusSeries(name='CurrentClampStimulus',
                                                                electrode=ic_electrode, conversion=1e-9,
                                                                gain=1.0, data=ci,
                                                                starting_time=ci_start_time, rate=ci_fs)
        ci_nwb.add_acquisition(current_stim)

        # -- CurrentInjection
        self.insert1(dict(key, nwb_current_stim=ci_nwb,
                          current_injection=ci,
                          current_injection_start_time=ci_start_time,
                          current_injection_sampling_rate=ci_fs))
        nwb.close()


