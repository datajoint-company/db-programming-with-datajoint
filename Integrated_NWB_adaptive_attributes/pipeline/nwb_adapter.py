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
        try:
            with NWBHDF5IO(save_fp.as_posix(), mode='w') as io:
                io.write(nwb)
                print(f'Write NWB 2.0 file: {save_file_name}')
            return save_fp.as_posix()
        except Exception as e:
            if save_fp.exists():
                save_fp.unlink()
            raise e

    def get(self, path):
        io = NWBHDF5IO(str(pathlib.Path(path)), mode='r')
        nwb = io.read()
        nwb.io = io
        return nwb


nwb_file = NWBFile()


class PatchClampSeries(dj.AttributeAdapter):
    """ Adapter for: pynwb.icephys.PatchClampSeries"""
    attribute_type = 'filepath@nwb_store'

    def put(self, nwb):
        patch_clamp = [obj for obj in nwb.objects.values()
                       if obj.neurodata_type == 'PatchClampSeries'][0]

        save_file_name = ''.join([nwb.identifier + '_{}'.format(patch_clamp.name), '.nwb'])
        save_fp = nwb_mp_dir / save_file_name
        try:
            with NWBHDF5IO(save_fp.as_posix(), mode='w', manager=nwb.io.manager) as io:
                io.write(nwb)
                print(f'Write PatchClampSeries: {save_file_name}')
            return save_fp.as_posix()
        except Exception as e:
            if save_fp.exists():
                save_fp.unlink()
            raise e

    def get(self, path):
        io = NWBHDF5IO(str(pathlib.Path(path)), mode='r')
        nwb = io.read()
        patch_clamp = [obj for obj in nwb.objects.values()
                       if obj.neurodata_type == 'PatchClampSeries'][0]
        patch_clamp.io = io
        return patch_clamp


patch_clamp_series = PatchClampSeries()
