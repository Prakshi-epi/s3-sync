import pandas as pd
import string
import os
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.snowball import SnowballStemmer
import enchant
import argparse
from subprocess import call
import csv
import codecs

parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-i', type=str, help='input file')
parser.add_argument('-o', type=str, help='Final Output File')
args = vars(parser.parse_args())

filename = args['i']
output_file = args['o']
normal_d = enchant.Dict('en_US')


# Lemmatizing and stemming of word

def lemstem(word, lemma=False, stem=False):
    """     Lemmatizing and stemming of word

    :param word: Token in our data set
    :param lemma: if it's True, word lemmatizing takes place
    :param stem: if it's True, word stemming takes place
    :return: string
    """
    if lemma:
        lemmatizer = WordNetLemmatizer()
        return str(lemmatizer.lemmatize(word))
    if stem:
        stemmer = SnowballStemmer('english')
        return str(stemmer.stem(word))
    else:
        return ''


def percentage_counter(word, digit=False, punc=False, upper=False, lower=False):
    """
    Gives the %age of digits or punctuations or upper-case letters or lower-case letters
    Input: Word
    Output: %age
    """

    counter = 0
    if len(word) == 0:
        return 0.0

    if digit:
        for i in word:
            if i.isdigit():
                counter += 1
        return float(counter / float(len(word)))

    elif punc:
        counter = sum([char in string.punctuation for char in word])
        return float(counter / float(len(word)))

    elif upper:
        for i in word:
            if i.isupper():
                counter += 1
        return float(counter / float(len(word)))

    elif lower:
        for i in word:
            if i.islower():
                counter += 1
        return float(counter / float(len(word)))

    else:
        return 0


def process_word(word):
    if type(word) == float:
        return str(word).encode('ascii', 'ignore').decode('utf-8', 'ignore')

    word = word.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return word


def creating_dataframe(names):
    """ This function takes in list of tokens and does feature engineering for each token
    and returns the dataframe
    :param names: list of tokens
    :return: dataframe containing features of each token
    """
    ExistinDict = [normal_d.check(word) for word in names]  # check if word is present in a dictionary
    names = [process_word(word) for word in names]  # convert it into string
    # print names[:50]
    last_3 = [words[-3:] for words in names]  # last 3 characters of the word
    last_2 = [words[-2:] for words in names]  # last 2 characters of the word
    length = [len(words) for words in names]  # length of the word
    Lowercase = [words.lower() for words in names]
    Uppercase = [words.upper() for words in names]
    Digit = [words.isdigit() for words in names]  # Is word a number?
    Title = [words.istitle() for words in names]  # Is word a Title?
    Vowel_count = [sum(map(words.lower().count, "aeiou")) for words in names]  # No. of vowels present in a word
    digit_in_words = [any(char.isdigit() for char in word) for word in names]  # No. of digits present in a word
    HasPunctuation = [any(char in string.punctuation for char in word) for word in names]  # word contain punctuation
    AllPunctuations = [all(char in string.punctuation for char in word) for word in names]  # all are punctuation
    percentageDigit = [percentage_counter(word, digit=True) for word in names]  # percentage of digit in a word
    percentagePunc = [percentage_counter(word, punc=True) for word in names]  # percentage of punctuattion in a word
    percentageUpper = [percentage_counter(word, upper=True) for word in names]  # percentage of uppercase letters
    percentagelower = [percentage_counter(word, lower=True) for word in names]  # percentage of lowercase letters
    Lemma = [lemstem(process_word(word), lemma=True) for word in names]  # lemmatizing
    stem = [lemstem(process_word(word), stem=True) for word in names]  # stemming

    df = pd.DataFrame(names)
    df.columns = ['names']
    dat = df.assign(last_3=last_3, last_2=last_2, length=length, Lowercase=Lowercase,
                    Uppercase=Uppercase, Digit=Digit, Title=Title, Vowel_count=Vowel_count,
                    digit_in_words=digit_in_words, HasPunctuation=HasPunctuation,
                    AllPunctuations=AllPunctuations, percentageDigit=percentageDigit,
                    percentagePunc=percentagePunc, percentageUpper=percentageUpper,
                    percentagelower=percentagelower, ExistinDict=ExistinDict, Lemma=Lemma, Stem=stem)

    return dat


def creating_textfiles(path, df):
    """ Creating text file in which dataframe is stored which is needed for CRF model.

    :param path: path to store the dataframe into text file
    :param df: dataframe we want to store
    """

    df.to_csv(path, header=None, index=None, sep=" ", mode='a', quotechar=" ", encoding='utf-8')


# Converting into Oneliner File
root = "/mnt/tmp/mallet/mallet-input"
os.chdir(root)
print 'Dir changed'
processed_file = 'processed_' + filename
command = "perl -pe 's/\s/\n/g;s/[\;]/\n/g;s/[\.]/\n/g;s/[\,]/\n/g;s/[\:]/\n/g' < " + filename + ' > ' + processed_file
call(command, shell=True)
print 'File Created : ' + processed_file + ' in the input folder'

path = root + '/' + processed_file  # Change when no oneliners
test_data = []
with codecs.open(path, 'rb', encoding='utf-8') as f:
    test_data = f.read().split()

test_data = [word.strip() for word in test_data]

# Calling Function
df = creating_dataframe(test_data)

# Column names in a particular order
columnsTitles = ['names', 'AllPunctuations', 'HasPunctuation', 'Digit', 'ExistinDict', 'Lemma', 'Stem', 'Lowercase',
                 'Title', 'Uppercase', 'Vowel_count', 'digit_in_words', 'last_2', 'last_3', 'length',
                 'percentageDigit', 'percentagePunc', 'percentageUpper', 'percentagelower']

df = df.reindex(columns=columnsTitles)  # Reindexing Columns according to above order
# print df.head()

# Storing Output containing features
path_to_txt = '/mnt/tmp/mallet/feature_Archive/' + output_file
creating_textfiles(path_to_txt, df)  # Calling function

print 'File Created: ' + output_file + ' in the feature_Archive directory'
print 'PreProcessing.py executed'
