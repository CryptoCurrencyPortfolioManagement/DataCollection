""" Copyright 2017, Dimitrios Effrosynidis, All rights reserved. """
import re
import string
import numpy as np
from functools import partial
from collections import Counter
import nltk
import pandas as pd
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

import datetime as dt
import sys

import pysentiment2 as ps
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

cc_df = pd.read_excel("./data/cc_survival_2017_top50.xlsx", index_col= 0)
cc_list = cc_df["Symbol"].str.lower()
cc_all_list = cc_df["Symbol"].str.lower()
cc_dict = {}
for alt_column in ["Name", "Alt Name", "Alt Symbol"]:
    cc_all_list.append(cc_df[alt_column].str.lower())
    temp_dict = dict(zip(cc_df[alt_column].str.lower(), cc_df["Symbol"].str.lower()))
    cc_dict = {**cc_dict, **temp_dict}

from gensim.models import KeyedVectors


# preprocessing
lemmatizer = WordNetLemmatizer() # set lemmatizer
stoplist = stopwords.words('english')
my_stopwords = "multiexclamation multiquestion multistop url atuser st rd nd th am pm"
stoplist = stoplist + my_stopwords.split()

def removeUnicode(text):
    """ Removes unicode strings like "\u002c" and "x96" """
    text = re.sub(r'(\\u[0-9A-Fa-f]+)',r'', text)       
    text = re.sub(r'[^\x00-\x7f]',r'',text)
    return text

def replaceURL(text):
    """ Replaces url address with "url" """
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','url',text)
    text = re.sub(r'#([^\s]+)', r'\1', text)
    return text

def replaceAtUser(text):
    """ Replaces "@user" with "atUser" """
    text = re.sub('@[^\s]+','atUser',text)
    return text

def removeHashtagInFrontOfWord(text):
    """ Removes hastag in front of a word """
    text = re.sub(r'#([^\s]+)', r'\1', text)
    return text

def removeCashtagInFrontOfWord(text):
    """ Removes hastag in front of a word """
    text = re.sub(r'\$([^\s]+)', r'\1', text)
    return text

def removeNumbers(text):
    """ Removes integers """
    text = ''.join([i for i in text if not i.isdigit()])         
    return text

def replaceMultiExclamationMark(text):
    """ Replaces repetitions of exlamation marks """
    text = re.sub(r"(\!)\1+", ' multi Exclamation ', text)
    return text

def replaceMultiQuestionMark(text):
    """ Replaces repetitions of question marks """
    text = re.sub(r"(\?)\1+", ' multi Question ', text)
    return text

def replaceMultiStopMark(text):
    """ Replaces repetitions of stop marks """
    text = re.sub(r"(\.)\1+", ' multi Stop ', text)
    return text

def countMultiExclamationMarks(text):
    """ Replaces repetitions of exlamation marks """
    return len(re.findall(r"(\!)\1+", text))

def countMultiQuestionMarks(text):
    """ Count repetitions of question marks """
    return len(re.findall(r"(\?)\1+", text))

def countMultiStopMarks(text):
    """ Count repetitions of stop marks """
    return len(re.findall(r"(\.)\1+", text))

def countElongated(text):
    """ Input: a text, Output: how many words are elongated """
    regex = re.compile(r"(.)\1{2}")
    return len([word for word in text.split() if regex.search(word)])

def countAllCaps(text):
    """ Input: a text, Output: how many words are all caps """
    return len(re.findall("[A-Z0-9]{3,}", text))

""" Creates a dictionary with slangs and their equivalents and replaces them """
with open('./models/slang.txt') as file:
    slang_map = dict(map(str.strip, line.partition('\t')[::2])
    for line in file if line.strip())

slang_words = sorted(slang_map, key=len, reverse=True) # longest first for regex
regex = re.compile(r"\b({})\b".format("|".join(map(re.escape, slang_words))))
replaceSlang = partial(regex.sub, lambda m: slang_map[m.group(1)])

def countSlang(text):
    """ Input: a text, Output: how many slang words and a list of found slangs """
    slangCounter = 0
    slangsFound = []
    tokens = nltk.word_tokenize(text)
    for word in tokens:
        if word in slang_words:
            slangsFound.append(word)
            slangCounter += 1
    return slangCounter, slangsFound

""" Replaces contractions from a string to their equivalents """
contraction_patterns = [ (r'won\'t', 'will not'), (r'can\'t', 'cannot'), (r'i\'m', 'i am'), (r'ain\'t', 'is not'), (r'(\w+)\'ll', '\g<1> will'), (r'(\w+)n\'t', '\g<1> not'),
                         (r'(\w+)\'ve', '\g<1> have'), (r'(\w+)\'s', '\g<1> is'), (r'(\w+)\'re', '\g<1> are'), (r'(\w+)\'d', '\g<1> would'), (r'&', 'and'), (r'dammit', 'damn it'), (r'dont', 'do not'), (r'wont', 'will not') ]
def replaceContraction(text):
    patterns = [(re.compile(regex), repl) for (regex, repl) in contraction_patterns]
    for (pattern, repl) in patterns:
        (text, count) = re.subn(pattern, repl, text)
    return text

def replaceElongated(word):
    """ Replaces an elongated word with its basic form, unless the word exists in the lexicon """

    repeat_regexp = re.compile(r'(\w*)(\w)\2(\w*)')
    repl = r'\1\2\3'
    if wordnet.synsets(word):
        return word
    repl_word = repeat_regexp.sub(repl, word)
    if repl_word != word:      
        return replaceElongated(repl_word)
    else:       
        return repl_word

def removeEmoticons(text):
    """ Removes emoticons from text """
    text = re.sub(':\)|;\)|:-\)|\(-:|:-D|=D|:P|xD|X-p|\^\^|:-*|\^\.\^|\^\-\^|\^\_\^|\,-\)|\)-:|:\'\(|:\(|:-\(|:\S|T\.T|\.\_\.|:<|:-\S|:-<|\*\-\*|:O|=O|=\-O|O\.o|XO|O\_O|:-\@|=/|:/|X\-\(|>\.<|>=\(|D:', '', text)
    return text

def countEmoticons(text):
    """ Input: a text, Output: how many emoticons """
    return len(re.findall(':\)|;\)|:-\)|\(-:|:-D|=D|:P|xD|X-p|\^\^|:-*|\^\.\^|\^\-\^|\^\_\^|\,-\)|\)-:|:\'\(|:\(|:-\(|:\S|T\.T|\.\_\.|:<|:-\S|:-<|\*\-\*|:O|=O|=\-O|O\.o|XO|O\_O|:-\@|=/|:/|X\-\(|>\.<|>=\(|D:', text))


### Spell Correction begin ###
""" Spell Correction http://norvig.com/spell-correct.html """
def words(text): return re.findall(r'\w+', text.lower())

WORDS = Counter(words(open('./models/corporaForSpellCorrection.txt').read()))

def P(word, N=sum(WORDS.values())): 
    """P robability of `word`. """
    return WORDS[word] / N

def spellCorrection(word): 
    """ Most probable spelling correction for word. """
    return max(candidates(word), key=P)

def candidates(word): 
    """ Generate possible spelling corrections for word. """
    return (known([word]) or known(edits1(word)) or known(edits2(word)) or [word])

def known(words): 
    """ The subset of `words` that appear in the dictionary of WORDS. """
    return set(w for w in words if w in WORDS)

def edits1(word):
    """ All edits that are one edit away from `word`. """
    letters    = 'abcdefghijklmnopqrstuvwxyz'
    splits     = [(word[:i], word[i:])    for i in range(len(word) + 1)]
    deletes    = [L + R[1:]               for L, R in splits if R]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R)>1]
    replaces   = [L + c + R[1:]           for L, R in splits if R for c in letters]
    inserts    = [L + c + R               for L, R in splits for c in letters]
    return set(deletes + transposes + replaces + inserts)

def edits2(word): 
    """ All edits that are two edits away from `word`. """
    return (e2 for e1 in edits1(word) for e2 in edits1(e1))

### Spell Correction End ###

### Replace Negations Begin ###

def replace(word, pos=None):
    """ Creates a set of all antonyms for the word and if there is only one antonym, it returns it """
    antonyms = set()
    for syn in wordnet.synsets(word, pos=pos):
      for lemma in syn.lemmas():
        for antonym in lemma.antonyms():
          antonyms.add(antonym.name())
    if len(antonyms) == 1:
      return antonyms.pop()
    else:
      return None

def replaceNegations(text):
    """ Finds "not" and antonym for the next word and if found, replaces not and the next word with the antonym """
    i, l = 0, len(text)
    words = []
    while i < l:
      word = text[i]
      if word == 'not' and i+1 < l:
        ant = replace(text[i+1])
        if ant:
          words.append(ant)
          i += 2
          continue
      words.append(word)
      i += 1
    return words

### Replace Negations End ###

def addNotTag(text):
    """ Finds "not,never,no" and adds the tag NEG_ to all words that follow until the next punctuation """
    transformed = re.sub(r'\b(?:not|never|no)\b[\w\s]+[^\w\s]',
       lambda match: re.sub(r'(\s+)(\w+)', r'\1NEG_\2', match.group(0)),
       text,
       flags=re.IGNORECASE)
    return transformed

def addCapTag(word):
    """ Finds a word with at least 3 characters capitalized and adds the tag ALL_CAPS_ """
    if(len(re.findall("[A-Z]{3,}", word))):
        word = word.replace('\\', '' )
        transformed = re.sub("[A-Z]{3,}", "ALL_CAPS_"+word, word)
        return transformed
    else:
        return word


def tokenize(text, token):
    finalTokens = []

    tokens = nltk.word_tokenize(text) # it takes a text as an input and provides a list of every token in it
    for w in tokens:

        if (w not in stoplist): # Technique 10: remove stopwords
            #cc conversion
            if w in cc_dict.keys():
                w = cc_dict[w]
            if w == token.lower():
                w = "token"
            w = lemmatizer.lemmatize(w) # Technique 14: lemmatizes words

            finalTokens.append(w)

    onlyOneSentence = " ".join(finalTokens)  # form again the sentence from the list of tokens

    return onlyOneSentence

def tokenize_lite(text, token):
    finalTokens = []

    tokens = nltk.word_tokenize(text) # it takes a text as an input and provides a list of every token in it

    for w in tokens:

        if (w not in stoplist): # Technique 10: remove stopwords
            #cc conversion
            if w in cc_dict.keys():
                w = cc_dict[w]
            if w == token.lower():
                w = "token"
            #final_word = replaceElongated(final_word) # Technique 11: replaces an elongated word with its basic form, unless the word exists in the lexicon
            finalTokens.append(w)

    onlyOneSentence = " ".join(finalTokens)  # form again the sentence from the list of tokens

    return onlyOneSentence


global totalSlangsFound
totalSlangsFound = []

import time

def preprocessor(text, token):
    text = removeUnicode(text)

    text = replaceURL(text)  # Technique 1
    text = replaceAtUser(text)  # Technique 1
    text = removeHashtagInFrontOfWord(text)  # Technique 1
    text = removeCashtagInFrontOfWord(text) # Technique 1

    text = replaceContraction(text)  # Technique 3: replaces contractions to their equivalents

    text = replaceMultiExclamationMark(text)  # Technique 5: replaces repetitions of exlamation marks with the tag "multiExclamation"
    text = replaceMultiQuestionMark(text)  # Technique 5: replaces repetitions of question marks with the tag "multiQuestion"
    text = replaceMultiStopMark(text)  # Technique 5: replaces repetitions of stop marks with the tag "multiStop"

    text = tokenize(text, token)

    return text

def preprocessor_lite(text, token):
    text = removeUnicode(text)

    text = tokenize_lite(text, token)

    return text

fasttext_model = KeyedVectors.load_word2vec_format("./models/crawl-300d-2M.vec")

def generateW2V(text):
    vectors = list()
    for word in text.split(" "):
        if word in fasttext_model:
            vectors.append(fasttext_model.get_vector(word, norm=True))
    if len(vectors) > 0:
        vectors = np.stack(vectors)
        return vectors
    else:
        return np.stack([[0]*300])

def generateW2VToken(text):
    vectors = list()
    for word in text.split(" "):
        if word in fasttext_model:
            vectors.append(fasttext_model.key_to_index[word])
    return vectors


lm_analyzer = ps.LM()

def generateLMSentimentScore(text):
    tokens = lm_analyzer.tokenize(text)
    score = lm_analyzer.get_score(tokens)
    return score["Polarity"]


vader_analyzer = SentimentIntensityAnalyzer()
def generateVaderSentimentScore(text):
    score = vader_analyzer.polarity_scores(text)
    return score["compound"]
