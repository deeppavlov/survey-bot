import scipy.stats as s
import configparser
import pandas as pd
from nltk import word_tokenize
import nltk
nltk.download('punkt')

config = configparser.ConfigParser()
config.read('bot.ini')
config = config['DEFAULT']

data = pd.read_csv(config['out_tsv'], sep='\t')


def compare(data):
    res = {}
    for g, d in data.groupby(['is_bot', 'is_meaningful']):
        x = d['answer'].apply(lambda x: len(word_tokenize(x)))
        res[g] = (x > 0).sum()

    obs = [res[(1, 0)], res[(1, 1)]]
    exp = [res[(0, 0)], res[(0, 1)]]

    chi = s.chisquare(obs, f_exp=exp)
    return chi.pvalue, [obs, exp]


print('overall', len(data), compare(data))

for g, d in data.groupby('chat_id'):
    try:
        print(g, len(d), compare(d))
    except KeyError as ex:
        pass