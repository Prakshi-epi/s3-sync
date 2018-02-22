import codecs
import argparse
import spacy
from itertools import groupby
from subprocess import call
import os
import random
import pandas as pd
import glob
import sys

random.seed(1)

MAXLENGTH = 50
MINLENGTH = 10

parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-i', type=str, help='Input Folder')
parser.add_argument('-o', type=str, help='Output Folder')

args = vars(parser.parse_args())
input_file = args['i']
Output_path = args['o']

# Input Files
data_folder = glob.glob(input_file + '*.txt')
random.shuffle(data_folder)


# Encoding word
def process_word(word):
    if type(word) == float:
        return str(word).encode('ascii', 'ignore').decode('utf-8', 'ignore')

    word = word.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return word


def entity_extraction_spacy(wl, ll):
    """
        Function will take label list as an input and generate groups and indexes based on the consecutive
        occurrence of labels
    :param wl: list of all tokens
    :param ll: list of all labels predicted by the spacy model
    :return: final_idx - list contains text, start_offset, end_offset and type
    """

    final_idxs = []
    # list of consecutive elements
    groups = [list(g) for _, g in groupby(ll)]  

    idxs = []
    s = 0
    e = 0

    # indexes of consecutive elements
    for g in groups:
        e += len(g)
        idxs.append([s, e])
        s = e

    # list of elements and their ranges
    for g, ix in zip(groups, idxs):
        if 'O' not in g:
            # add the char offsets plus the whitespaces
            start_offset = sum([len(e) for e in wl[:ix[0]]]) + ix[0]
            end_offset = sum([len(e) for e in wl[:ix[1]]]) + ix[1] - 1
            token = ' '.join(e for e in wl[ix[0]:ix[1]])
            final_idxs.append([token, start_offset, end_offset, g[0]])  # start, end, tag

    return final_idxs


def testing(nlp2, path, output_path):
    """
        Function will read textfile and will create list of sentences. Now. list of sentences
        will be given as input to spacy model and prediction are stored in json format
    :param nlp2: contains Spacy Model
    :param path: Input File
    :param output_path: Output File path

    """

    # Converting Text file into List of list of words in a sentence
    newX = []
    sentences = []
    count = 0
    for line in codecs.open(path, 'r', "utf-8"):
        try:
            words = process_word(line.strip())
            sentences.append(words)
            count += 1
            # Break at max length, or add logic to break at punctuation
            if len(sentences) == MAXLENGTH or "." in words and len(sentences) > MINLENGTH:
                # Append to training data
                newX.append(sentences)
                sentences = []

        except:  # error for irrelevant blank spaces
            pass

    # Last sentence
    if len(sentences) != 0:
        newX.append(sentences)
    
    # Generating List of sentences
    final = []
    # print newX[:2]
    for wl in newX:
        conv_wl = u" ".join(i for i in wl)
        final.append(conv_wl)

    print('Number of words: ' + str(count))

    test_wl = []
    test_ll = []

    # Prediction
    for text in final:
        # if '""""' in text:
        #     print(text)
        doc = nlp2(text)
        # print('Entities', [(ent.text, ent.label_) for ent in doc.ents])
        # print('Tokens', [(t.text, t.ent_type_, t.ent_iob) for t in doc])
        for t in doc:
            if t.text == '  ' or t.text == ' ' or t.text == '-':
                test_ll.append('O')
                test_wl.append('')
            else:
                if t.ent_type_ == u'' or t.ent_type_ == u' ':
                    test_ll.append('O')
                else:
                    test_ll.append(t.ent_type_)
                test_wl.append(t.text)

    # Calling Function to generate list of predicted tokens, start-offsets, end-offsets & types
    test_output = entity_extraction_spacy(test_wl, test_ll)
    # print(test_output[:20])

    # test_output : contains start_offset, end_offset and tag
    test_output = pd.DataFrame(test_output)
    test_output.columns = ['text', 'start', 'end', 'type']
    test_output.to_json('/home/nlpepimum/Test/EMR/spacy_predict/output/' + output_path, orient="records")  # Converting DataFrame to json


# Converting into oneliner file
file_path = "/home/nlpepimum/Test/EMR/spacy_predict/input"
os.chdir(file_path)

# load the saved model
out_dir = '/home/nlpepimum/Test/EMR/model310'
print("Loading from", out_dir)
nlp2 = spacy.load(out_dir)


# Predicting labels for all the files in a folder
for f in data_folder:
    # File names
    print f
    
    # Converting text file into one word per line
    processed_file = 'processed_' + os.path.basename(f)
    command = "perl -pe 's/\s/\n/g;s/[\;]/\n/g;s/[\.]/\n/g;s/[\,]/\n/g;s/[\:]/\n/g' < " + os.path.basename(f) \
              + ' > ' + processed_file
    call(command, shell=True)

    print 'File Created : ' + processed_file + ' in the input folder'

    print 'Testing Started....'
    path = file_path + '/' + processed_file
    
    outputfile = os.path.basename(f).split('.')[0] + '.json'
    try:
	# Calling function
    	testing(nlp2, path, outputfile)
	print 'File Created : ' + outputfile + ' in the json_output folder'
    except:
	# Error
	print('Error: ', sys.exc_info()[0])
