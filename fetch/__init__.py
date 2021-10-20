#!/usr/bin/env python3

import os
import pandas as pd
import yaxil
import collections
import json
import logging
from nipype.interfaces.dcm2nii import Dcm2niix
from nipype.interfaces.fsl import ExtractROI

logger = logging.getLogger(__name__)

def authenticate():
    auth = yaxil.auth(alias='cbscentral', cfg='~/.cbsauth')
    return auth

def get_scan_metadata(auth, subject_cbs_id):
    data = []
    try:
        with yaxil.session(auth) as sess:
            for e in sess.experiments(label=subject_cbs_id):
                for s in sess.scans(experiment=e):
                    data += [pd.DataFrame.from_dict(s, orient='index')]       

    except Exception as e:
        logger.exception(e)

    return data

def save_scan_metadata(study_dir, subject_bids_id, data):
    source_path = get_source_path(study_dir, subject_bids_id)
    p = os.path.join(source_path, subject_bids_id + '.csv')
    try:
        data = pd.concat(data, axis=1).T
        data['scan_num'] = pd.to_numeric(df['id'])
        data.sort_values('scan_num')
        data.to_csv(p, header=True, index=False, sep=',')

    except Exception as e:
        logger.exception(e)
        logger.error('Could not concatenate, sort, and save the data. Exiting.')
        raise   
 
    return data

def get_behavioral_data(auth, subject_cbs_id):
    try:
        with yaxil.session(auth) as sess:
            for e in sess.experiments(label=subject_cbs_id):
                behavioral_data = []
                try:
                    _, res = yaxil._get(sess._auth, experiment.uri + '/files', yaxil.Format.JSON)
                    result = res['ResultSet']['Result']
                    behavioral_data = [r for r in result if r['collection'] == 'behavioral_task_data']
                except Exception as e:
                    logger.exception(e)
                finally:
                    yield behavioral_data

    except Exception as e:
        logger.exception(e)

def save_behavioral_data(auth, study_dir, subject_bids_id, data):
    try:
        with yaxil.session(auth) as sess:
            for d in data:
                res = bytes('', 'utf8')
                uri = d['URI']
                source_path = get_source_path(study_dir, subject_bids_id)
                file_path = get_file_name(uri, source_path, 'behavioral')

                try:
                    _, res = yaxil._get(sess._auth, uri, yaxil.Format.JSON, autobox=False)

                except Exception as e:
                    logger.exception(e)

                finally:
                    with open(file_path, 'wb') as f:
                        logger.info(f'Writing {file_path}')
                        f.write(res)

    except Exception as e:
        logger.exception(e)

def get_file_name(uri, source_path, file_type):
    basename = uri.split('files/')[-1]
    output_dir = source_path

    if file_type == 'behavioral':
        output_dir = os.path.join(output_dir, 'behavioral_files')
        basename = convert_basename_behavioral(basename)

    file_path = os.path.join(output_dir, basename)
    return file_path

def convert_basename_behavioral(basename):
    reference = {
        'CARIT_RUN1' : 'CARIT_Run_1',
        'CARIT_RUN2' : 'CARIT_Run_2',
        'EMOTION' : 'EMOTION_Run_1',
        'GUESSING_RUN1' : 'GUESSING_Run_1',
        'GUESSING_RUN2' : 'GUESSING_Run_2',
        'WORKING_MEMORY' : 'WM_Run_1'
    }
    b = basename.upper()

    if b in reference:
        return reference[b]

    else:
        logger.error('Cannot convert the behavioral filename. Please update the reference.')
        raise

def get_scan_data(auth, subject_cbs_id, subject_bids_id, metadata, study_dir):
    runs = collections.defaultdict(int)

    for row in metadata:
        scan_id = row['id']
        description = row['series_description']
        runs[description] += 1
        run = runs[description]
        save_scan_data(auth, subject_cbs_id, subject_bids_id, scan_id, study_dir, description, run)
        save_fmap(study_dir, subject_bids_id)

def get_opp_direction(direction):
    if direction == 'ap':
        return 'pa'
    elif direction == 'AP':
        return 'PA'
    elif direction == 'PA':
        return 'AP'
    elif direction == 'pa':
        return 'ap'
    else:
        logger.error(f'Direction {direction} is not compatible. Please check the filename')
        raise

def get_direction(nii_path):
    try:
        return nii_path.split('/')[-1].split('_dir-')[1].split('_')[0]
    except Exception as e:
        logger.exception(e)
        logger.error(f'Could not parse direction from {nii_path}.')
        raise

def get_phase_encoding_direction(nii_path):
    b = nii_path.replace('.nii.gz', '.json')
    try:
        with open(b) as f:
            return json.load(f)['PhaseEncodingDirection']
    except Exception as e:
        logger.exception(e)
        logger.error(f'Could not get PhaseEncodingDirection from file {b}')
        raise

def save_phase_encoding_direction(output_json, nii_path, phase_encoding_direction):
    nii = os.path.basename(nii_path)

    metadata = collections.defaultdict()
    metadata['IntendedFor'] = os.path.join('func', nii)
    metadata['PhaseEncodingDirection'] = phase_encoding_direction
    j = json.dumps(metadata)

    try:
        with open(output_json, 'w') as f:
            f.write(j)

    except Exception as e:
        logger.exception(e)
        logger.error(f'Could not save phase encoding direction metadata {output_json}.')
        raise

def save_fmap(study_dir, subject_bids_id):
    subject_dir = os.path.join(study_dir, subject_bids_id)
    fmap_dir = os.path.join(subject_dir, 'fmap')
    func_dir = os.path.join(subject_dir, 'func')
    bolds = glob.glob(os.path.join(func_dir, '*bold.nii.gz'))

    for b in bolds:
        direction = get_direction(b)
        opposite_direction = get_opp_direction(direction)
        opposite_func = b.replace(direction, opposite_direction)

        if not os.path.exists(opposite_func):
            continue

        epi_nii = b.replace('bold', 'epi').replace('func', 'fmap')
        epi_json = epi_nii.replace('.nii.gz', '.json')
        ExtractROI(in_file=b, roi_file=epi_nii, t_min=0, t_size=10, output_type='NIFTI_GZ')
        
        phase_encoding_direction = get_phase_encoding_direction(b)
        save_phase_encoding_direction(epi_json, opposite_func, phase_encoding_direction)

def save_scan_data(auth, subject_cbs_id, subject_bids_id, scan_id, study_dir, description, run):
    try:
        source_path = get_source_path(study_dir, subject_bids_id)
        dcm_dir = os.path.join(source_path, 'dicom')
        scan_dir = os.path.join(dcm_dir, scan_id)
        os.makedirs(scan_dir, exist_ok=True)

        with yaxil.session(auth) as sess:
            sess.download(subject_cbs_id, scan_ids=[scan_id], out_dir=scan_dir, progress=True)
        
        nii_path = get_nii_path(study_dir, subject_bids_id, run, description)
        convert_dcm_to_nii(scan_dir, nii_path)

    except Exception as e:
        logger.exception(e)
        logger.error('Could not save scan data.')
        raise
 
def convert_dcm_to_nii(scan_dir, nii_path):
    try:
        converter = Dcm2niix()
        converter.inputs.source_dir = scan_dir
        converter.inputs.out_filename = os.path.basename(nii_path)
        converter.inputs.output_dir = os.path.dirname(nii_path)
        converter.inputs.single_file = True
        converter.inputs.bids_format = True
        converter.inputs.compress = 'y'
        logger.info(converter.cmdline)
        converter.run()

    except Exception as e:
        logger.exception(e)

def get_fmri_filepath(study_dir, subject_bids_id, task, direction, run, scan_type)
    nii_filename = '{s}_task-{t}_dir-{d}_run-{r}_{st}.nii.gz'.format(s=subject_bids_id,
        t=task, d=direction, r=run, st=scan_type)
    return os.path.join(study_dir, subject_bids_id, 'func', nii_filename)

def get_dmri_filepath(study_dir, subject_bids_id, direction, run, scan_type):
    nii_filename = '{s}_dir-{d}_run-{r}_{st}.nii.gz'.format(s=subject_bids_id,
        d=direction, r=run, st=scan_type)
    return os.path.join(study_dir, subject_bids_id, 'dwi', nii_filename)

def get_nii_path(study_dir, subject_bids_id, run, description):
    try:
        d = description.split('_')

        if d[0].endswith('fMRI'):
            if d[-1].lower() == 'sbref':
                task = d[-3].lower()
                direction = d[-2].lower()
                scan_type = d[-1].lower()

            else:
                task = d[-2].lower()
                direction = d[-1].lower()
                scan_type = 'bold'

            return get_fmri_filepath(study_dir, subject_bids_id, task, direction, run, scan_type)
 
        elif d[0].endswith('dMRI'):
            if d[-1].lower() == 'sbref':
                scan_type = d[-1].lower()
                direction = d[-2].lower()

            else:
                scan_type = 'dwi'
                direction = d[-1].lower()

            return get_dmri_filepath(study_dir, subject_bids_id, direction, run, scan_type)

        else:
            logger.error(f'Incompatible type {d[0]}. Exiting.')
            raise   
 
    except Exception as e:
        logger.exception(e)
        raise

def get_source_path(study_dir, subject_bids_id):
    return os.path.join(study_dir, 'sourcedata', subject_bids_id)
