#!/usr/bin/env python3

import os
import logging
import argparse as ap
from datetime import datetime

import fetch as f
import preprocessing as p
import util as u

log_file = f'STAR-{t}.log'
logging.basicConfig(filename=log_file, format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger('star_logger')

def main():
    parser = ap.ArgumentParser(description='STAR pipeline')
    parser.add_argument('--bids_dir', help='BIDS directory path',
        default='/mnt/stressdevlab/STAR')
    parser.add_argument('--cbs_ids', nargs='+',
        help='subject CBS ID on XNAT. If unspecified, all subjects will be processed.')
    parser.add_argument('--container_dir', help='container directory',
        default='/mnt/stressdevlab/scripts/Containers')
    parser.add_argument('--fmriprep_ver', help='fMRIprep container version', required=True)
    parser.add_argument('--xcpengine_ver', help='xcpengine container version', required=True)
    parser.add_argument('--run', help='modules to run', nargs='+', 
        choices=['download', 'fmriprep', 'confounds', 'behavioral', 'xcpengine', 'model'],
        default=['download', 'fmriprep', 'confounds', 'behavioral', 'xcpengine', 'model']
    )
    args = parser.parse_args()

    t = datetime.now()
    log_file = f'STAR-{t}.log'
    logger.info('Preprocessing has begun. Parsing arguments.')

    # get arguments
    study_dir = get_study_dir(args.bids_dir)
    cbs_ids = get_subject_cbs_id(args.cbs_ids)
    container_dir = get_container_dir(args.container_dir)
    fmriprep_version = get_fmriprep_ver(container_dir, args.fmriprep_ver)
    xcpengine_version = get_xcpengine_ver(container_dir, args.xcpengine_ver)
    modules = list(args.run)

    n = len(cbs_ids)

    if not n:
        logger.critical('Subjects not found.')
        return

    logger.info('Parsing arguments complete. Processing {} subjects.'.format(n))

    for subject_cbs_id in cbs_ids:

        logger.info(f'Preprocessing subject {subject_cbs_id}')

        # download fmri and behavioral data
        if 'download' in modules:
            logger.info(f'Downloading data for subject {subject_cbs_id}')
            download(study_dir, subject_cbs_id)

        # fmriprep
        if 'fmriprep' in modules:
            logger.info(f'Running fMRIprep for subject {subject_cbs_id}')
            run_fmriprep(study_dir, subject_cbs_id, fmriprep_version, container_dir, omp_threads_num,
                threads_num, fd_spike_threshold, fs_license_path, cifti, output_spaces)

        # filter confounds
        if 'confounds' in modules:
            logger.info(f'Processing confounds for subject {subject_cbs_id}')
            process_fmriprep_confounds(study_dir, subject_cbs_id, fmriprep_version)

        # preprocess behavioral
        if 'behavioral' in modules:
            logger.info(f'Processing behavioral data for subject {subject_cbs_id}')
            process_onsets(study_dir, subject_cbs_id)

        # xcpengine
        if 'xcpengine' in modules:
            logger.info(f'Running xcpengine for subject {subject_cbs_id}')
            run_xcpengine(study_dir, subject_cbs_id, fmriprep_version, xcpengine_version, 
                container_dir)

def get_study_dir(path):
    if not os.path.exists(path):
        logger.critical(f'{path} does not exist.')
        raise
    return path

def get_subject_cbs_id(cbs_ids):
    arr = []
    for i in cbs_ids:
        try:
            c = i.split('_')
        except Exception as e:
            logger.exception(e)
            logger.error(f'Could not parse {i}')
            continue
        if not c[1] == 'STAR':
            logger.error(f'{i} is not a STAR subject.')
            continue
        elif len(c) != 4:
            err = f'{i} is invalid CBS ID. The format must be {YYMMDD}_STAR_{BIDSID}_{num}'
            logger.error(err)
            continue
        arr.append(i)
    return arr

def get_container_dir(container_dir):
    if not os.path.exists(container_dir):
        logger.critical(f'{container_dir} does not exist.')
        raise

    return container_dir

def get_fmriprep_ver(container_dir, fmriprep_ver):
    fmriprep_container = os.path.join(container_dir, 'fmriprep-{}.simg'.format(fmriprep_ver))
    if not os.path.exists(fmriprep_container):
        logger.critical(f'{fmriprep_container} does not exist.')
        raise

    return fmriprep_ver

def get_xcpengine_ver(container_dir, xcpengine_ver):
    xcpengine_container = os.path.join(container_dir, 'xcpengine-{}.simg'.format(xcpengine_ver))
    if not os.path.exists(xcpengine_container):
        logger.critical(f'{xcpengine_container} does not exist.')
        raise

    return xcpengine_ver

def get_subject_bids_id(subject_cbs_id):
    try:
        s = subject_cbs_id.split('_')
        return 'sub-{}'.format(s[2] + s[3])
    except Exception as e:
        logger.exception(e)
        logger.critical(f'Could not get bids id for {subject_cbs_id}.')
        raise

def download(study_dir, subject_cbs_id):
    auth = f.authenticate()
    subject_bids_id = get_subject_bids_id(subject_cbs_id)    

    scan_metadata = f.get_scan_metadata(auth, subject_cbs_id)
    f.save_scan_metadata(study_dir, subject_bids_id, scan_metadata)

    behavioral_data = f.get_behavioral_data(auth, subject_cbs_id)
    f.save_behavioral_data(auth, study_dir, subject_bids_id, behavioral_data)
    
    f.get_scan_data(auth, subject_cbs_id, subject_bids_id, scan_metadata, study_dir)
    u.morphometrics(subject_cbs_id, subject_bids_id, study_dir, fmriprep_version)

def run_fmriprep(study_dir, subject_cbs_id, fmriprep_version, container_dir,
    omp_threads_num, threads_num, fd_spike_threshold, fs_license_path, cifti, output_spaces):

    subject_bids_id = get_subject_bids_id(subject_cbs_id)

    fmriprep_command = p.fmriprep.get_singularity_command(study_dir, subject_bids_id, 
        fmriprep_version, container_dir, omp_threads_num, threads_num, fd_spike_threshold, 
        fs_license_path, cifti, output_spaces)
    p.fmriprep.run_sbatch(study_dir, subject_bids_id, fmriprep_version, fmriprep_command)

def process_fmriprep_confounds(study_dir, subject_cbs_id, fmriprep_version):
    subject_bids_id = get_subject_bids_id(subject_cbs_id)
    p.fmriprep.filter_confounds(study_dir, subject_bids_id, fmriprep_version)

def process_onsets(study_dir, subject_cbs_id):
    subject_bids_id = get_subject_bids_id(subject_cbs_id)
    p.behavioral.emotion_onsets(study_dir, subject_bids_id)
    p.behavioral.guessing_onsets(study_dir, subject_bids_id)
    p.behavioral.carrit_onsets(study_dir, subject_bids_id)
    p.behavioral.wm_onsets(study_dir, subject_bids_id)

def run_xcpengine(study_dir, subject_cbs_id, fmriprep_version, xcpengine_version, container_dir):

    subject_bids_id = get_subject_bids_id(subject_cbs_id)   
 
    p.xcpengine.prepare_cohort_file(study_dir, subject_bids_id, fmriprep_version)
    xcpengine_command = p.xcpengine.get_singularity_command(study_dir, subject_bids_id,
        xcpengine_version, fmriprep_version, container_dir)
    p.xcpengine.run_sbatch(study_dir, subject_bids_id, fmriprep_version, 
        xcpengine_command)

if __name__=='__init__':
    main()
