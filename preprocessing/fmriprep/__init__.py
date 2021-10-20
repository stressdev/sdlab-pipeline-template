#!/usr/bin/env python3

import os
import glob
import pandas as pd
import numpy as np

def get_fmriprep_dir(study_dir, fmriprep_version):
    return os.path.join(study_dir, 'derivatives', 'fmriprep-{}'.format(fmriprep_version))

def get_work_dir(study_dir, fmriprep_version):
    return os.path.join(study_dir, 'derivatives', 'fmriprep-work-{}'.format(fmriprep_version))

def get_log_dir(study_dir, fmriprep_version):
    return os.path.join(study_dir, 'derivatives', 'fmriprep-log-{}'.format(fmriprep_version))

def get_sbatch_dir(study_dir, fmriprep_version):
    return os.path.join(study_dir, 'derivatives', 'fmriprep-sbatch-{}'.format(fmriprep_version))

def get_singularity_command(study_dir, subject_bids_id, fmriprep_version, container_dir, 
        omp_threads_num, threads_num, fd_spike_threshold, fs_license_path, cifti, output_spaces):

    fmriprep_dir = get_fmriprep_dir(study_dir, fmriprep_version)
    work_dir = get_work_dir(study_dir, fmriprep_version)
    subject_work_dir = os.path.join(work_dir, subject_bids_id)
    
    cmd = ['singularity', 'run', '--cleanenv',
            '-B', '{}:/work'.format(subject_work_dir),
            '-B', study_dir, container_dir,
            '--ignore', 'slicetiming',
            '--participant-label', subject_bids_id,
            '-vvv', '--omp-nthreads', omp_threads_num,
            '--nthreads', threads_num,
            '-w', '/work', '--return-all-components',
            '--fd-spike-threshold', fd_spike_threshold,
            '--fs-license-file', fs_license_path,
            '--cifti-output', cifti,
            '--output-spaces', ' '.join(output_spaces),
            '--skip-bids-validation', study_dir, fmriprep_dir, 'participant']

    return ' '.join(cmd)

def run_sbatch(study_dir, subject_bids_id, fmriprep_version, cmd):
    try:
        sbatch_dir = get_sbatch_dir(study_dir, 'derivatives', 'sbatch', 'fmriprep')
        sbatch_file_path = os.path.join(sbatch_dir, subject_bids_id + '.sbatch')

        with open(sbatch_file_path) as f:
            f.writelines('#!/bin/bash\n')
            f.writelines('#SBATCH --job-name=fmriprep\n')
            f.writelines('#SBATCH --output={}/%x_%j.out\n'.format(sbatch_dir))
            f.writelines('#SBATCH --error={}/%x_%j.err\n'.format(sbatch_dir))
            f.writelines('#SBATCH --time=02-00:30:30\n')
            f.writelines('#SBATCH -n 1\n')
            f.writelines('#SBATCH --cpus-per-task=8\n')
            f.writelines('#SBATCH --mem-per-cpu=4G\n')
            f.writelines('#SBATCH --partition=ncf\n')
            f.writelines(cmd)
    
        os.system('sbatch {}'.format(sbatch_file_path))

    except Exception as e:
        print(e)
        print('Could not run sbatch.')
        raise

def filter_confounds(study_dir, subject_bids_id, fmriprep_version):
    fmriprep_dir = get_fmriprep_dir(study_dir, fmriprep_version)
    subject_fmriprep_dir = os.path.join(fmriprep_dir, 'fmriprep', subject_bids_id)

    confounds = os.path.join(subject_fmriprep_dir, 'func', '*task*desc-confounds_regressors.tsv')
    subject_confounds = glob.glob(confounds)

    if not subject_confounds:
        print('fmriprep confounds not found for subject {}.'.format(subject_bids_id))
        raise
    
    try:
        for c in subject_confounds:
            df = pd.read_csv(c, sep = '\t')
            df.reset_index(inplace = True)
            params = ['trans_x', 'trans_y', 'trans_z', 
                'rot_x', 'rot_y', 'rot_z', 'csf', 'white_matter', 'global_signal']
            df[params].to_csv(c.replace('.tsv', '-9p.txt'), header = None, index = None, sep = ' ')

            #Get framewise displacement > 0.5
            fd_outliers = df[df['framewise_displacement'] > 0.5][['index','framewise_displacement']]
            o = c.replace('confounds_regressors.tsv','fd_outliers_0pt5.txt')
            fd_outliers[['index']].to_csv(o, index = False, header = False)

            #Get dvars outliers (> 75th percentile + (1.5 * IQR))
            dvars = [float(x) for x in df['dvars'].values[1:]]
            q25, q75 = np.percentile(dvars, 25), np.percentile(dvars, 75)
            iqr = q75 - q25
            cut_off = iqr * 1.5
            lower, upper = q25 - cut_off, q75 + cut_off
            dvars_outliers = df[df['dvars'] > upper][['dvars','index']]
            d = c.replace('confounds_regressors.tsv','dvars_outliers.txt')
            dvars_outliers[['index']].to_csv(d, index = False, header = False)

    except Exception as e:
        print(e)
        print('Could not filter confounds.')
        raise
