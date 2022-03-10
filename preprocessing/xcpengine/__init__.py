#!/usr/bin/env python

import preprocessing as p

def get_scan_files(subject_fmriprep_dir, subject_bids_id):
    scan_files = []

    files_of_interest = [
        'task-rest_dir-ap_run-1_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz',
        'task-rest_dir-pa_run-1_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz'
    ]
    
    for f in files_of_interest:
        preproc = '{s}_{foi}'.format(s=subject_bids_id, foi=f)
        filepath = os.path.join(subject_fmriprep_dir, 'func', preproc)
        
        if not os.path.exists(filepath):
            print('File {} does not exist.'.format(filepath))
            continue

        scan_files.append(filepath)

    return scan_files   

def get_cohort_file(study_dir, subject_bids_id):
    return os.path.join(study_dir, 'derivatives', 
        'ind_cohort_files', '{}.csv'.format(subject_bids_id))

def prepare_cohort_file(study_dir, subject_bids_id, fmriprep_version):
    fmriprep_dir = p.fmriprep.get_fmriprep_dir(study_dir, fmriprep_version)
    subject_fmriprep_dir = os.path.join(fmriprep_dir, 'fmriprep', subject_bids_id)
    
    scan_files = get_scan_files(subject_fmriprep_dir, subject_bids_id)
    if not scan_files:
        print('Compatible cohort files not found.')
        raise

    try:
        cohort_file = get_cohort_file(study_dir, subject_bids_id)

        with open(cohort_file) as f:
            f.writelines('id0,img\n')
        
            for s in scan_files:
                f.writelines(s + '\n')

    except Exception as e:
        print(e)
        print('Could not create a cohort file.')
        raise 

def get_dsn_path(study_dir):
    return os.path.join(study_dir, 'derivatives', 'fc-36p.dsn')

def get_derivatives_dir(study_dir):
    return os.path.join(study_dir, 'derivatives')

def get_xcpengine_dir(study_dir, fmriprep_version):
    fmriprep_dir = p.fmriprep.get_fmriprep_dir(study_dir, fmriprep_version)
    return os.path.join(fmriprep_dir, 'xcpengine')

def get_singularity_command(study_dir, subject_bids_id, 
        xcpengine_version, fmriprep_version, container_dir):

    cohort_file = get_cohort_file(study_dir, subject_bids_id)
    dsn = get_dsn_path(study_dir)
    xcpengine_dir = get_xcpengine_dir(study_dir, fmriprep_version)
    derivatives_dir = get_derivatives_dir(study_dir)

    cmd = ['singularity', 'run', '--cleanenv',
            '-B', study_dir, container_dir,
            '-d', dsn,
            '-c', cohort_file,
            '-o', xcpengine_dir,
            '-t 2',
            '-r', derivatives_dir
    ]

    return ' '.join(cmd) 

def run_sbatch(study_dir, subject_bids_id, fmriprep_version, cmd):
    try:
        sbatch_dir = get_sbatch_dir(study_dir, 'derivatives', 'sbatch', 'xcpengine')
        sbatch_file_path = os.path.join(sbatch_dir, subject_bids_id + '.sbatch')

        with open(sbatch_file_path) as f:
            f.writelines('#!/bin/bash\n')
            f.writelines('#SBATCH --job-name=xcpengine\n')
            f.writelines('#SBATCH --output={}/%x-%A-%a.out\n'.format(sbatch_dir))
            f.writelines('#SBATCH --error={}/%x-%A-%a.err\n'.format(sbatch_dir))
            f.writelines('#SBATCH --time=10:00:00\n')
            f.writelines('#SBATCH -n 1\n')
            f.writelines('#SBATCH --cpus-per-task=1\n')
            f.writelines('#SBATCH --mem-per-cpu=20G\n')
            f.writelines('#SBATCH --partition=ncf\n')
            f.writelines(cmd)

        os.system('sbatch {}'.format(sbatch_file_path))

    except Exception as e:
        print(e)
        print('Could not run sbatch.')
        raise
