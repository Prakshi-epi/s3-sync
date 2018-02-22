import pandas as pd
import codecs
import argparse


parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-f', type=str, help='Feature File')
parser.add_argument('-l', type=str, help='label File')
parser.add_argument('-o', type=str, help='Output File')
args = vars(parser.parse_args())

feature = args['f']
labelfile = args['l']
outputfile = args['o']


# Encoding word
def process_word(word):
    if type(word) == float:
        return str(word).encode('ascii', 'ignore').decode('utf-8', 'ignore')

    word = word.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return word


def prediction(log):
    """ Creating list of predicted labels
    :param log: path to the label file
    :return: Predicted Label list
    """
    df = pd.read_csv(log, names=['label'], encoding='utf-8')
    df['label'] = df['label'].map(lambda x: str(x)[:-1])  # Removing whitespace from each row
    df.drop(df.index[[0, 1]], inplace=True)  # Removing the first two rows
    df = df.label.tolist()  # Converting data frame into list
    # print df[:5]
    print 'Length of Prediction: ' + str(len(df))
    return df


def feat(path):
    """ Creating list of tokens
    :param path: path to the feature file
    :return: Token list
    """
    # Reading testing Data set
    feature = pd.read_csv(path, header=None, delimiter=' ', quotechar=' ', encoding='utf-8', low_memory=False)
    feature = feature.loc[:, 0]  # Removing all columns except first one
    print 'length of token file'
    print feature.shape
    # print feature[:5]
    # print len(feature)
    return feature


def creating_outputfile(token, label):
    data = []
    start = 0

    for word, tag in zip(token, label):
        if type(word) == float:
            end = len(str(word)) + start
        else:
            end = len(word) + start
        data.append([process_word(word), start, end, tag])
        start = end + 1

    # print data[:50]
    print 'length of data:' + str(len(data))

    final_set = []
    for val in range(len(data)):
        s_val = val
        if data[val][3].split('-')[0] == 'B':
            start = data[val][1]
            word = data[val][0]
            tag = data[val][3].split('-')[1]
            s_val = s_val + 1
            if s_val >= len(data):
                break
            while data[s_val][3].split('-')[0] == 'I':
                word = word + ' ' + data[s_val][0]
                s_val = s_val + 1
                if s_val >= len(data):
                    break

            end = data[s_val - 1][2]
            final_set.append([process_word(word), start, end, tag])

        elif data[val][3].split('-')[0] == 'I':
            continue

    print final_set[-1]

    # with codecs.open('/home/tahir/json_output/' + outputfile, 'w', 'utf-8') as f:
    #     for tokens, start, end, tag in final_set:
    #         f.write('{}\t{}\t{}\t{}\n'.format(tokens, start, end, tag))

    final_set = pd.DataFrame(final_set)
    final_set.columns = ['text', 'start', 'end', 'type']
    final_set.to_json('/mnt/tmp/mallet/mallet-output/'+outputfile, orient="records")


root = '/mnt/tmp/mallet/feature_Archive/'
token_path = root + feature
token = feat(token_path)  # Calling Function

label_path = root + labelfile
label = prediction(label_path)  # Calling Function

# Generating
creating_outputfile(token, label)
print 'File Created : ' + outputfile + ' in the output folder'
