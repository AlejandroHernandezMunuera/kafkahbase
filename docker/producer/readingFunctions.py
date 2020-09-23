#!/usr/bin/python3

import os
import tarfile
import subprocess
import numpy as np

#Extract data from tar files
def untarFiles(rootPath):
    for root, dir, files in os.walk(rootPath):
            for file in files:
                file_path = os.path.join(root, file)
                if(tarfile.is_tarfile(file_path)):
                    print('Processing ' + file_path)
                    tar = tarfile.open(file_path)
                    tar.extractall(rootPath)
                    tar.close()


subFolders = ['SPAM', 'HAM']

#Return the list with filepaths for each mail with the label
#LABELS BASED ON SUBGOLDER NAME
def obtain_mailPathList(folderName, untar=False, labeled=False):
    X = []
    for label in subFolders:
        path = os.path.join(folderName, label)
        
        if(untar):
            untarFiles(path)
        file_str = subprocess.check_output('find {} -type f'.format(path), shell=True, encoding='utf-8')
        if(labeled):
            file_list = [ [x,label] for x in file_str.splitlines() if not x.endswith(("tar.gz",".tar", ".DS_Store"))]
        else:
            file_list = [ x for x in file_str.splitlines() if not x.endswith(("tar.gz",".tar", ".DS_Store"))]
        
        X.extend(file_list)
    return np.asarray(X)

#Extract content from email file
def readFilepath(i):
    content = open(i, encoding = "ISO-8859-1").read()
    return content
