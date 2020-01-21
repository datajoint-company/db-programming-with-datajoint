import datajoint as dj
import pathlib
from pynwb import NWBHDF5IO

exported_nwb_dir = dj.config['stores']['nwb_store']['stage']

nwb_session_dir = pathlib.Path(exported_nwb_dir, 'session')
nwb_mp_dir = pathlib.Path(exported_nwb_dir, 'membrane_potential')

nwb_session_dir.mkdir(parents=True, exist_ok=True)
nwb_mp_dir.mkdir(parents=True, exist_ok=True)


class NWBFile(dj.AttributeAdapter):
    """ Adapter for: pynwb.file.NWBFile"""
    attribute_type = 'filepath@nwb_store'

    def put(self, nwb):
        save_file_name = ''.join([nwb.identifier, '.nwb'])
        save_fp = nwb_session_dir / save_file_name

        print(f'Write PatchClampSeries: {save_file_name}')
        _write_nwb(save_fp, nwb)
        return save_fp.as_posix()

    def get(self, path):
        io = NWBHDF5IO(str(pathlib.Path(path)), mode='r')
        nwb = io.read()
        nwb.io = io
        return nwb


class PatchClampSeries(dj.AttributeAdapter):
    """ Adapter for: pynwb.icephys.PatchClampSeries"""
    attribute_type = 'filepath@nwb_store'

    def put(self, nwb):
        patch_clamp = [obj for obj in nwb.objects.values()
                       if obj.neurodata_type == 'PatchClampSeries'][0]

        save_file_name = ''.join([nwb.identifier + '_{}'.format(patch_clamp.name), '.nwb'])
        save_fp = nwb_mp_dir / save_file_name

        print(f'Write PatchClampSeries: {save_file_name}')
        _write_nwb(save_fp, nwb, manager=nwb.io.manager)
        return save_fp.as_posix()

    def get(self, path):
        io = NWBHDF5IO(str(pathlib.Path(path)), mode='r')
        nwb = io.read()
        patch_clamp = [obj for obj in nwb.objects.values()
                       if obj.neurodata_type == 'PatchClampSeries'][0]
        patch_clamp.io = io
        return patch_clamp


class CurrentClampStimulusSeries(dj.AttributeAdapter):
    """ Adapter for: pynwb.icephys.CurrentClampStimulusSeries"""
    attribute_type = 'filepath@nwb_store'

    def put(self, nwb):
        current_stim = [obj for obj in nwb.objects.values()
                        if obj.neurodata_type == 'CurrentClampStimulusSeries'][0]

        save_file_name = ''.join([nwb.identifier + '_{}'.format(current_stim.name), '.nwb'])
        save_fp = nwb_mp_dir / save_file_name

        print(f'Write CurrentClampStimulusSeries: {save_file_name}')
        _write_nwb(save_fp, nwb, manager=nwb.io.manager)
        return save_fp.as_posix()

    def get(self, path):
        io = NWBHDF5IO(str(pathlib.Path(path)), mode='r')
        nwb = io.read()
        current_stim = [obj for obj in nwb.objects.values()
                        if obj.neurodata_type == 'CurrentClampStimulusSeries'][0]
        current_stim.io = io
        return current_stim


# ---- instantiate dj.AttributeAdapter objects ----

nwb_file = NWBFile()
patch_clamp_series = PatchClampSeries()
current_stim_series = CurrentClampStimulusSeries()


# ============= HELPER FUNCTIONS ===============

def _write_nwb(save_fp, nwb2write, manager=None):
    try:
        with NWBHDF5IO(save_fp.as_posix(), mode='w', manager=manager) as io:
            io.write(nwb2write)
    except Exception as e:
        if save_fp.exists():
            save_fp.unlink()
        raise e
