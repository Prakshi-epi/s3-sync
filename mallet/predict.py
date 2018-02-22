import os
import subprocess
from subprocess import call
import codecs
import argparse
import glob
import random

random.seed(1)

parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-i', type=str, help='Input Folder')
parser.add_argument('-f', type=str, help='Feature Folder')
parser.add_argument('-o', type=str, help='Output Folder')

args = vars(parser.parse_args())
input_file = args['i']
feature_path = args['f']
Output_path = args['o']

# Input Files
data_folder = glob.glob(input_file + '*.txt')
random.shuffle(data_folder)

# Path to Mallet
mallet_exec = "java -cp "
input_path = "/mallet-2.0.8/class:/mallet-2.0.8/lib/mallet-deps.jar"
model_file = 'nouncrf'  # Model File
mallet_path = '/mallet-2.0.8'

# Path to python scripts
file_path = "/mnt/tmp/mallet"

# Predicting Labels For all the files in input Folder
# count = 0
for f in data_folder:

    # Feature Extraction 
    os.chdir(file_path)
    feature = 'feature_' + os.path.basename(f) + '.txt'
    command = 'python /PreProcessing_Test.py -i ' + os.path.basename(f) + ' -o ' + feature
    call(command, shell=True)

    # Data including all features = feature_path + feature
    
    # Mallet prediction (only labels in the output file)
    os.chdir(mallet_path)
    Output_p = '/mallet_predict/feature_Archive/' + os.path.basename(f).split('.')[0]
    command = mallet_exec + input_path + " cc.mallet.fst.SimpleTagger" + " --model-file " + model_file + " " + feature_path + feature
    call(command, shell=True, stdout=open(Output_p, 'a'), stderr=subprocess.STDOUT)

    # Final Output in JSON using feature file(only token column needed) and mallet prediction file (only labels needed)
    os.chdir(file_path)
    json_output = os.path.basename(f).split('.')[0] + '.json'
    command = 'python /editing.py -f ' + feature + ' -l ' + os.path.basename(f).split('.')[0] \
              + ' -o ' + json_output
    call(command, shell=True)
    count += 1
