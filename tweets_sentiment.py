import sys
import string
import re
import MapReduce

mr = MapReduce.MapReduce()
scores = {}
count=1

def mapper(each_tweet):
    global count
    key = count
    count = count + 1
    value=each_tweet['text']
    value = re.sub(r"http\S+", "", value)
    value =re.sub('RT @\S+','',value)
    value =re.sub('@\S+','',value)
    value =re.sub('#\S+','',value)
    value =re.sub(r'[?|$|.|!|\\|@|#|-|(|)]','',value).strip()

    value = value.lower().strip()
    words = re.findall(r'[\w+]+',value)
    for w in words:
        if w in scores:
           mr.emit_intermediate(key,scores.get(w))
        else:
           mr.emit_intermediate(key,0)








def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += v
    if total == 0:
        mr.emit((key, total))
    else:
        mr.emit((key, float(total)))





if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)  # Convert the score to an integer.
    tweet_data = open(sys.argv[2])
    mr.execute(tweet_data, mapper, reducer)


