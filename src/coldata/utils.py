import numpy as np
import os
import pickle
import torch


def check_exists(path):
    return os.path.exists(path)


def makedir_exist_ok(path):
    os.makedirs(path, exist_ok=True)
    return


def save(input, path, mode='torch'):
    dirname = os.path.dirname(path)
    makedir_exist_ok(dirname)
    if mode == 'torch':
        torch.save(input, path)
    elif mode == 'np':
        np.save(path, input, allow_pickle=True)
    elif mode == 'pickle':
        with open(path, 'wb') as file:
            pickle.dump(input, file)
    else:
        raise ValueError('Not valid save mode')
    return


def load(path, mode='torch'):
    if mode == 'torch':
        output = torch.load(path, weights_only=False)
    elif mode == 'np':
        output = np.load(path, allow_pickle=True)
    elif mode == 'pickle':
        with open(path, 'rb') as file:
            output = pickle.load(file)
    else:
        raise ValueError('Not valid save mode')
    return output
