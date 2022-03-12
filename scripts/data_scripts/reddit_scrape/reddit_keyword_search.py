import sys, praw, nltk, string, pandas
from nltk.corpus import stopwords
from nltk.collocations import *


subreddits  = ['anxiety', 'anxietyhelp', 'healthanxiety', 'panicattack', 'socialanxiety']
commonWords = {'', '.', '?', '!', '*', "&"}

redditUsername = None #"[YOUR USERNAME HERE]"
redditPassword = None #"[YOUR PASSWORD HERE]"

clientID = "[YOUR CLIENT ID HERE]"
clientSecret = "[YOUR SECRET HERE]"
userAgent = "personal use script"

reddit = praw.Reddit(   client_id=clientID,
                        client_secret=clientSecret,
                        user_agent=userAgent,
                        username=redditUsername,
                        password=redditPassword,
                        ratelimit_seconds=600 )

stopWords = set(stopwords.words('english'))
word_frequency = {}
bigram_frequency = {}
trigram_frequency = {}

#finding collocations
bigram_measures = nltk.collocations.BigramAssocMeasures()
trigram_measures = nltk.collocations.TrigramAssocMeasures()

def gatherCorpus(subreddit, numOfPosts):
    submissionCounter = 0

    for submission in reddit.subreddit(subreddit).top(limit=numOfPosts):
        submissionCounter += 1
        print("Scanning submission %d of %d" % (submissionCounter, numOfPosts))

        #get rid of stopwords
        title_tokens = sanitiizeWords(submission.title) #process the title
        text_tokens = sanitiizeWords(submission.selftext) #process the submission text
       
        #generate bigrams
        bigramFinder = nltk.collocations.BigramCollocationFinder.from_words(text_tokens)
        bigrams = sorted(bigramFinder.nbest(bigram_measures.raw_freq,100))

        #generate trigrams
        trigramFinder = nltk.collocations.TrigramCollocationFinder.from_words(text_tokens)
        trigrams = sorted(trigramFinder.nbest(trigram_measures.raw_freq,100))

        #add frequencies
        addToWordFrequency(title_tokens)
        addToWordFrequency(text_tokens)
        addtoPhraseFrequency(bigrams)
        addtoPhraseFrequency(trigrams, True)

def addToWordFrequency(list):
    for word in list:
        word_frequency[word] = word_frequency.get(word, 0) + 1

def addtoPhraseFrequency(tuple_list, isTrigram=False):
    for tuple_item in tuple_list:
        item = list(tuple_item)
        phrase = " ".join(item)

        dictionary = trigram_frequency if isTrigram else bigram_frequency
        dictionary[phrase] = dictionary.get(phrase, 0) + 1

def sanitiizeWords(text):
    text = text.translate(str.maketrans('', '', string.punctuation))
    text_list = text.lower().split(' ')  #nltk.word_tokenize(title)

    for i in range(len(text_list)):
        if text_list[i] in stopWords or text_list[i] in commonWords: 
            text_list[i] = None
    
    return [i for i in text_list if i]

def getKeysValues(dictionary):
    return dictionary.keys(), dictionary.values()


def save():
    dict_list = [word_frequency, bigram_frequency, trigram_frequency]
    n = 1
    for item in dict_list:
        keys, values = getKeysValues(item)
        df = pandas.DataFrame({'keyword': keys, "occurences": values})
        indexNames = df[ df['occurences'] < 10 ].index
        df.drop(indexNames , inplace=True)
        df.to_csv(str(n) + ".csv", index=False)
        n = n+1

if __name__ == "__main__":
    if(len(sys.argv) < 1):
        print("Usage: Reddit_CommonWords.py <titles or comments> <subreddit> <number of posts to search> <number of words to display>")
        sys.exit()

    noPosts = int(sys.argv[1])

    for subreddit in subreddits:
        gatherCorpus(subreddit, noPosts)

    save()