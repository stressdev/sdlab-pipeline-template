#!/usr/bin/env python3

import os
import pandas as pd

def get_source_path(study_dir, subject_bids_id):
    return os.path.join(study_dir, 'sourcedata', subject_bids_id)

def get_behavioral_data(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(e)
        print('Could not read behavioral data {}'.format(file_path))
        raise

def get_output_headers(output_path):
    if 'WM_RUN_1' in os.path.basename(output_path):
        return ['blockCueStartTime', 'duration', 'amplitude']
    else:
        return ['onset', 'duration', 'amplitude']

def save_onsets(output_path, subset):
    try:
        if len(subset) > 0:
            headers = get_output_headers(output_path)
            subset[headers].to_csv(output_path, sep=' ', header=None, index=None)
        else:
            output_path = output_path.replace('.txt', '-EMPTY.txt')
            subset.to_csv(output_path, sep=',', header=None, index=None)
    except Exception as e:
        print(e)
        print('Could not save {}'.format(output_path))
        raise

def emotion_onsets(study_dir, subject_bids_id):
    source_path = get_source_path(study_dir, subject_bids_id)
    behavioral_file = os.path.join(source_path, 'behavioral_files', 'EMOTION_RUN_1')

    b = get_behavioral_data(behavioral_file)
    data = []

    for n, row in b[b['cueStartTime'] > 0].iterrows():
        data.append((row['trialCondition'], row['trialStartTime'], '18', '1'))
    
    data = pd.DataFrame.from_records(data).dropna(axis=0)
    data.columns = ['cond', 'onset', 'duration', 'amplitude']

    for cond in ['shape', 'face']:
        output_path = behavioral_file + '_' + cond + '.txt'
        subset = data[(data['cond'] == cond)]
        save_onsets(output_path, subset)

def guessing_onsets(study_dir, subject_bids_id):
    source_path = get_source_path(study_dir, subject_bids_id)
    behavioral_file = os.path.join(source_path, 'behavioral_files', 'GUESSING_Run_')
    runs = ['1','2']
    phases = ['cue', 'guess', 'feedback']

    for r in runs:
        f = behavioral_file + str(r)
        b = get_behavioral_data(f)

        data = []
        for n, row in b.iterrows():
            cond = row['trialCondition']
            for p in phases:
                start = row[p + 'StartTime']
                end = row[p + 'EndTime']
                duration = end - start
                
                d = (p, cond, start, duration, 1)
                data.append(d)

        data = pd.DataFrame.from_records(data).dropna(axis = 0)
        data.columns = ['phase', 'cond', 'onset', 'duration', 'amplitude']
    
        for p in phases:
            save_guessing_onsets(f, p, data)

def save_guessing_onsets(output_path, phase, data):
    if phase == 'guess':
        o = output_path + '_' + phase + '.txt'
        subset = data[(data['phase'] == phase)]
        save_onsets(o, subset)
    else:
        if phase = 'cue':
            conditions = ['low', 'high']
        elif phase = 'feedback':
            conditions = ['lowWin', 'lowLose', 'highWin', 'highLose']
        else:
            print('Conditions for guessing onsets unclear.')
            raise

        for cond in conditions:
            o = output_path + '_' + phase + '_' + cond + '.txt'
            subset = data[(data['phase'] == phase) & (data['cond'].str.contains(cond))]
            save_onsets(o, subset)

def get_carrit_row(acc, cond, data):
    d = data[(data['corrRespMsg'] == acc) & (data['corrAns'] == cond)]
    rows = []

    for n, row in d.iterrows():
        start = row['shapeStartTime']
        end = row['shapeEndTime']
        duration = end - start
        trial_outcome = row['trialOutcome']
        nogo_cond = row['nogoCondition'] if cond == 'nogo' else ''
        rows.append((cond, trial_outcome, nogo_cond, acc, start, duration, '1'))

    return rows

def carrit_onsets(study_dir, subject_bids_id):
    source_path = get_source_path(study_dir, subject_bids_id)
    behavioral_file = os.path.join(source_path, 'behavioral_files', 'CARIT_Run_')
    runs = ['1','2']

    for r in runs:
        f = behavioral_file + str(r)
        b = get_behavioral_data(f)

        data = []
        data.extend(get_carrit_row('correct', 'go', b))
        data.extend(get_carrit_row('correct', 'nogo', b))
        data.extend(get_carrit_row('incorrect', 'go', b))
        data.extend(get_carrit_row('incorrect', 'nogo', b))

        data = pd.DataFrame.from_records(data).dropna(axis=0)
        data.colums = ['cond', 'trial_outcome', 'nogo_cond', 'acc', 'onset', 'duration', 'amplitude']

        for acc in ['correct', 'incorrect']:
            save_carrit_onsets(f, data, acc, 'go')
            save_carrit_onsets(f, data, acc, 'prevRewNogo')
            save_carrit_onsets(f, data, acc, 'neutralNogo')

def save_carrit_onsets(output_path, data, acc, cond):
    o = output_path + '_' + cond + '_' + acc + '.txt'
    key = 'cond' if cond == 'go' else 'nogo_cond'
    subset = data[(data['acc'] == acc) & (data[key] == cond)]
    save_onsets(o, subset)

def wm_onsets(study_dir, subject_bids_id):
    source_path = get_source_path(study_dir, subject_bids_id)
    behavioral_file = os.path.join(source_path, 'behavioral_files', 'WM_Run_1')
    b = get_behavioral_data(behavioral_file)
    categories = ['faces', 'objects']

    start = b[(b['trialImageStartTime'] > 0) & (b['blockCueStartTime'] > 0)][['blockCueStartTime', 
        'condition', 'category']].reset_index(drop = True)
    end = b[b['blockFixStartTime'] > 0][['blockFixStartTime']].reset_index(drop = True)

    data = pd.concat([start, end], axis = 1)
    data['duration'] = data.apply(lambda x: x['blockFixStartTime'] - x['blockCueStartTime'], axis = 1)
    data['amplitude'] = data.apply(lambda x: 1, axis = 1)

    for c in categories:
        for cond in ['0back', '2back']:
            o = behavioral_file + '_' + cond + '_' + p + '.txt'
            subset = data[(data['category'] == c) & (data['condition'] == cond)]
            save_onsets(o, subset)
