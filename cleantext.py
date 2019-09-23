#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse

__author__ = ""
__email__ = ""
_PUNCTUATION = frozenset(string.punctuation)
_ENDING_PUNC = frozenset(['.', '!', '?', ',', ';', ':'])

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

def remove_punc(token):
	"""Removes punctuation from start/end of token."""
	i = 0
	j = len(token) - 1
	idone = False
	jdone = False
	beginning_punc_list = []
	ending_punc_list = []
	while i <= j and not (idone and jdone):
		if token[i] in _PUNCTUATION and not idone and token[i] != '%':
			i += 1
		else:
			idone = True
		if token[j] in _PUNCTUATION and not jdone and token[j] != '%':
			if token[j] in _ENDING_PUNC:
				ending_punc_list.insert(0, token[j])
			j -= 1
		else:
			jdone = True
	return [""] if i > j else [token[i:(j+1)]] + ending_punc_list

def sanitize(text):
	# A list to keep track of all ending punctuations and where to insert these punctuations
	to_be_inserted = []
	# List to keep track of bigrams and trigrams, to be joined later
	bigrams_list = []
	trigrams_list = []
	unigrams_list = []
	offset = 0
	
	# Combine steps 1 and 5 by splitting by tabs, spaces, and newline characters, while lowercasing the string
	text_list = re.split('[ \n\t]+', text.lower())

	for i in range(len(text_list)):
		# Remove URL's
		word = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:.*))','', text_list[i])

		# Remove punctuation and save the ending punctuation in a list to be added after
		word_list = remove_punc(word)
		text_list[i] = word_list[0]
		if len(word_list) > 1:
			to_be_inserted.append((word_list[1:], i+1))
	
	# Insert ending punctuation to the text list for the parsed text
	for (ilist, index) in to_be_inserted:
		text_list[index+offset:index+offset] = ilist
		offset += len(ilist)
		
	# Filter out any blank elements
	for i in range(len(text_list) - 1, -1, -1):
		word = text_list[i]
		if not word:
			del text_list[i]

	# Create unigrams
	for i in range(len(text_list)):
		if text_list[i] not in _ENDING_PUNC:
			unigram = text_list[i]
			unigrams_list.append(unigram)
	
	# Create bigrams
	for i in range(len(text_list) - 1):
		if text_list[i] not in _ENDING_PUNC and text_list[i + 1] not in _ENDING_PUNC:
			bigram = text_list[i] + '_' + text_list[i + 1]
			bigrams_list.append(bigram)
		
	# Create trigrams
	for i in range(len(text_list) - 2):
		if text_list[i] not in _ENDING_PUNC and text_list[i + 1] not in _ENDING_PUNC and text_list[i + 2] not in _ENDING_PUNC:
			trigram = text_list[i] + '_' + text_list[i + 1] + '_' + text_list[i + 2]
			trigrams_list.append(trigram)
	
	return unigrams_list + bigrams_list + trigrams_list


if __name__ == "__main__":
	text = sanitize("hello\nhi. my name is justin. I love python!!")
	print(text)