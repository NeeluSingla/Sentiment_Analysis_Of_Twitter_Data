import MapReduce
import sys
import re
import string


mr = MapReduce.MapReduce()
count = 1

def mapper(each_tweet):
   global count
   tweetwords={}
   key = count
   count = count + 1
   value=each_tweet['text']
   value = value.encode('utf-8','ignore').decode('utf-8').translate( string.punctuation)
   value = re.sub(r"http\S+", "", value)
   value =re.sub('RT @\S+','',value)
   value =re.sub('@\S+','',value)
   value =re.sub('#\S+','',value)
   value =re.sub(r'[?|$|.|!|@|#|-|(|)|-|:|+|\-|&|"]','',value).strip()
   value = value.lower()
   elements=value.split()
   for w in elements:
       if w in tweetwords.keys():
           tweetwords[w] = (tweetwords[w]) + 1
       else:
           tweetwords[w] = 1

   for w in tweetwords:
      mr.emit_intermediate(w,(key,tweetwords.get(w)))












def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total = total + 1
    mr.emit((key, total, list_of_values))






if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

