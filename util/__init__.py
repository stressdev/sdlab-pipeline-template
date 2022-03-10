#!/usr/bin/env python3

import os
import shutil
import errno
import glob

def morphometrics(subject_cbs_id, subject_bids_id, study_dir, fmriprep_version):
    #nrg_morphometrics = '/ncf/nrg/pipelines/CBSCentral/Morphometrics3/STAR_Study/'
    nrg_morphometrics = '/ncf/mclaughlin_lab_tier1/STAR/5_analysis_freesurfer/'
    subject_morphometrics_path = os.path.join(nrg_morphometrics, subject_cbs_id)
    #morphometrics_dir = glob.glob(subject_morphometrics_path + '/*/morphometrics')[0]
    morphometrics_dir = glob.glob(subject_morphometrics_path + '/*/morphometrics')[0]


    fmriprep_folder = 'fmriprep-{}'.format(fmriprep_version)
    #freesurfer_path = os.path.join(study_dir, 'derivatives', fmriprep_folder, 'freesurfer')
    #subject_morphometrics_dir = os.path.join(freesurfer_path, subject_bids_id, 'morphometrics')
    freesurfer_path = os.path.join(study_dir, 'derivatives', fmriprep_folder, 'freesurfer')
    subject_morphometrics_dir = os.path.join(freesurfer_path, subject_bids_id, 'morphometrics')

    #copy_morphometrics(morphometrics_dir, subject_morphometrics_dir)

    input_anat_file = os.path.join(subject_morphometrics_dir, 'T1.mgz')
    output_anat_directory = os.path.join(study_dir, subject_bids_id, 'anat')
    output_anat_file = os.path.join(output_anat_directory, subject_bids_id + '_T1w.nii.gz')
    mri_convert(input_anat_file, output_anat_file)

#def copy_morphometrics(morphometrics_dir, subject_morphometrics_dir):
#    try:
#        shutil.copytree(morphometrics_dir, subject_morphometrics_dir, dirs_exist_ok=True)
#    
#    except OSError as e:
#        if e.errno == errno.ENOTDIR:
#            shutil.copy(morphometrics_dir, subject_morphometrics_dir)
#        else:
#            print(e)
#            print('Could not copy morphometrics files. Exiting.')
#            raise

def mri_convert(in_file_path, out_file_path):
    try:
        mc = MRIConvert()
        mc.inputs.in_file = in_file_path
        mc.inputs.out_file = out_file_path
        mc.inputs.out_type = 'niigz'
        print(mc.cmdline)

    except Exception as e:
        print(e)
        print('Could not convert the file to nii.gz. Exiting.')
        raise
