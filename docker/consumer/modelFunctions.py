#!/usr/bin/python3

import re
#from sklearn.externals import joblib
import joblib
#import string
from string import punctuation
import nltk
#from nltk.corpus import stopwords 
nltk.download('punkt')
#nltk.download('stopwords')

joblib_file = "spam_model.pkl"

#Save model to file
def saveSpamModel(model):
    joblib.dump(model, joblib_file)

#Load model from file
def loadSpamModel():
    return joblib.load(joblib_file)

#Labels of the model
classes = ['SPAM', 'HAM']

#Stemming
stemmer = nltk.stem.porter.PorterStemmer()

#Tokenization and Stemming
def stem_tokens(tokens):
    #stopwords taken care of by sklearn vectorizers
    #stop_words = set(stopwords.words('english')) 
    #return [stemmer.stem(item) for item in tokens if not item in stop_words]
    
    return [stemmer.stem(item) for item in tokens]
    

#Text cleaning: punctuation, special characters and lowercase
#Tokenization and Stemming included
def clean_email(email):
    #TODO: clean better html format: BeautifulSoup's get_text() but hmtl content might be useful for the model
    #Remove special characters
    email = re.sub(r'\\r\\n', ' ', str(email))
    email = re.sub(r'\W', ' ', email)
    email = re.sub(r'http\S+', ' ', email)
    email = re.sub(r'\s+[a-zA-Z]\s+', ' ', email)
    email = re.sub(r'\^[a-zA-Z]\s+', ' ', email)
    email = re.sub(r'\s+', ' ', email, flags=re.I)
    email = re.sub("\d+", " ", email)
    email = email.replace('\n', ' ')
    email = email.translate(str.maketrans("", "", punctuation))
    email = email.lower()
    
    return stem_tokens(nltk.word_tokenize(email))

#Extract metadata from email
def info_part(i):
    return i.split('\n\n', 1)[0]

#Extract content from email
def content_part(i):
    mailDiv = i.split('\n\n', 1)
    if(len(mailDiv)<=1):
        return ''
    return mailDiv[1]

