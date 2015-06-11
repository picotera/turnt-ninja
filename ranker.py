from ConfigParser import SafeConfigParser
import math
import json
import logging
from threading import Thread
from Queue import PriorityQueue
import pyteaser
import time

import rabbitcoat
#from pygres import PostgresArticles
from helper import *

#TODO: Remove
import urllib
import pika

logging.getLogger('pika').setLevel(logging.WARNING)

WISHFUL_THINKING_EXPONENT = 0.1

dictToFloat = lambda x: {k: float(x[k]) for k in x}

QUERY_WEIGHT = 100.0
#TODO: Get the weights in a smarter way, like name less than ID or something
def weightsFromQuery(query):
    res = {}
    for val in query.values():
        res.update({word.lower(): QUERY_WEIGHT for word in val.split()})
    return res

SECTION_NAME = 'RANKER'
class Ranker(object):
    '''
    A script designed to rank articles coming from different sites
    The script receives the search results from other scripts, and ranks the articles.
    The script sends the results back to the manager.
    '''
    
    weight_key = 'weight'
    results_key = 'results'
    #TODO: Change this
    min_score = 1
    
    def __init__(self, config='conf/ranker.conf', rabbit_config='conf/rabbitcoat.conf', pygres_config='conf/pygres.conf'):
        self.logger = getLogger('ranker')
        
        self.logger.info("Initializing ranker")
        
        self.__loadConfig(config)
        
        self.queue = PriorityQueue()
        
        self.rankers = []
        for i in xrange(self.thread_count):
            thread = Thread(target=self.__rankerThread, args=())
            thread.name = thread.name = 'Ranker %s' %i
            
            thread.start()
            self.rankers.append(thread)
            
        #self.db_articles = PostgresArticles(self.logger, pygres_config)
        
        self.sender = rabbitcoat.RabbitSender(self.logger, rabbit_config, self.out_queue)
        self.receiver = rabbitcoat.RabbitReceiver(self.logger, rabbit_config, self.in_queue, self.__rabbitCallback)        
        self.receiver.start()
    
    def __loadConfig(self, config):
        parser = SafeConfigParser()
        parser.read(config)
        
        self.thread_count = parser.getint(SECTION_NAME, 'thread_count')
        
        weights_file = parser.get(SECTION_NAME, 'weights_file')
        with open(weights_file) as f:
            self._weights = dictToFloat(json.loads(f.read()))
        self.min_score = parser.getfloat(SECTION_NAME, 'min_score')        
        
        self.in_queue = parser.get(SECTION_NAME,'in_queue')
        self.out_queue = parser.get(SECTION_NAME,'out_queue')
    
    def __rabbitCallback(self, data, properties):
        '''
        This method adds a ranking to each item in the results
        @param data: Results of some query. Each element should have an article 'id' key
        '''
        self.logger.info('rabbitCallback handling message %s, %s' %(properties, data))
        corr_id = properties.correlation_id
        
        # This ain't supported, pass to the next queue
        if type(data) != dict or not data.get(RESULTS_KEY):
            self.sender.Send(data, corr_id=corr_id)
        
        self.queue.put((time.time(), data, corr_id))
    
    def __rankerThread(self):
        last_query = 0
        while True:
            q_time, data, corr_id = self.queue.get()
            self.logger.debug('Ranking request %s' %corr_id)
            try:
                # Load specific weights if supplied (name, id, country etc.)
                query = data.get(QUERY_KEY)
                if query:
                    weights = self._weights.copy()
                    weights.update(weightsFromQuery(query))
                else:
                    weights = self._weights
                
                # The keys will be striped, 
                for result in data[RESULTS_KEY]:
                    
                    id = result[ID_KEY]
                    article = getArticle(id)
                    #article = self.db_articles.GetArticle(id)
                    if (artice not from google):
                        article = " ".join(pyteaser.Summarize(title, text))

                    score = self.__rank(article, weights)
                    
                    self.logger.debug('Ranked result: %s, %s, %s' %(id, len(article), score))
                    
                    # Add the score to the result
                    result[SCORE_KEY] = score
                
                self.sender.Send(data, corr_id=corr_id)
            except Exception, e:
                self.logger.exception('Exception in rankerThread')
                self.queue.put((q_time, data))
                return
                
    def __rank(self, data, weights):
        return 100.0 * sum([data.count(word) * weight for word, weight in weights)]) / (len(data.split(" ")) * max(weights.values())
        
    #TODO: Delete this?
    def Rank(self, data, properties):
        self.__rabbitCallback(data, properties)

def getArticle(id):
    url = 'http://manager-cycurity.rhcloud.com/article?id=%s' %id
    f = urllib.urlopen(url)
    return f.read()
        
def fromDb(ranker, query, iterable):
    
    results = [{'id': i} for i in iterable]
    
    request = {QUERY_KEY: query,
               RESULTS_KEY: results}
    
    ranker.queue.put((time.time(), request))

def main():
    ranker = Ranker()
    
    '''
    data = {u'query': {u'origin': u'', u'first_name': u'Binyamin', u'last_name': u'Netanyahu', u'name': u'Binyamin Netanyahu', u'country': u'', u'id': u''}, u'results': [{u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 84, u'title': u'A dangerous modesty;  America and the Middle East'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 85, u'title': u'Sheldon Adelson looks to stamp out growing US movement to boycott Israel;  Billionaire gambling magnate and Republican party donor convenes closed-door meeting to combat US university movement amid growing Israeli alarm over growing Boycott, Divestment and Sanctions campaign in US and Europe'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 86, u'title': u'The Wrath of Netanyahu: What does Orange Telecom\u2019s departure from Israel really Mean for BDS?'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 87, u'title': u'BDS'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 88, u'title': u'Issue No.1249, 4 June, 2015 &nbsp03-06-2015; 10:50AM ET France tak...'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 89, u'title': u'Netanyahu: French Government Partially Owns Orange'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 90, u'title': u'Israel risk: Risk overview'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 91, u'title': u'Israel risk: Political stability risk'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 92, u'title': u'Israel risk: Security risk'}, {u'source': u'Negative News', u'query': u'Binyamin Netanyahu', u'id': 93, u'title': u'Israel risk: Alert - Risk scenario watchlist'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 94, u'title': u'Lebanon - Q3 2015'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 95, u'title': u'A dangerous modesty;  America and the Middle East'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 96, u'title': u'Poll: Most Saudis Believe Iran a Greater Threat than Israel'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 97, u'title': u'Israel risk: Alert - Worries over political uncertainty may increase'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 98, u'title': u'Israel risk: Alert - Coalition struggles may affect decision-making'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 99, u'title': u'Sheldon Adelson looks to stamp out growing US movement to boycott Israel;  Billionaire gambling magnate and Republican party donor convenes closed-door meeting to combat US university movement amid growing Israeli alarm over growing Boycott, Divestment and Sanctions campaign in US and Europe'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 100, u'title': u'Orange says it plans to terminate contract with brand partner in Israel;  French telecoms giant has been under pressure to end relationship with Partner over services to Israeli settlements regarded as illegal under international law'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 101, u'title': u'The Wrath of Netanyahu: What does Orange Telecom\u2019s departure from Israel really Mean for BDS?'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 102, u'title': u'BDS'}, {u'source': u'All News', u'query': u'Binyamin Netanyahu', u'id': 103, u'title': u'Orange says decision to pull out of Israel based on business, not politics - report'}, {u'source': u'World Compliance', u'query': u'Binyamin Netanyahu', u'id': 104, u'title': u'Netanyahu, Zila'}, {u'source': u'Info4C', u'query': u'Binyamin Netanyahu', u'id': 105, u'title': u'Netanyahu, Benjamin, Mr.'}, {u'source': u'Law Reviews', u'query': u'Binyamin Netanyahu', u'id': 106, u'title': u'Copyright (c) 2014 Arizona Board of Regents Arizona Law Review'}, {u'source': u'Law Reviews', u'query': u'Binyamin Netanyahu', u'id': 107, u'title': u'Copyright (c) 2014 Cornell University Cornell International Law Journal'}, {u'source': u'Law Reviews', u'query': u'Binyamin Netanyahu', u'id': 108, u'title': u'Copyright (c) 2015 Denver Journal of International Law and Policy Denver Journal of International Law and Policy'}, {u'source': u'Law Reviews', u'query': u'Binyamin Netanyahu', u'id': 109, u'title': u'Copyright (c) 2014 American Bar Foundation Law and Social Inquiry'}, {u'source': u'Law Reviews', u'query': u'Binyamin Netanyahu', u'id': 110, u'title': u'Copyright (c) 2013 Michigan Law Review Association Michigan Law Review'}]}

    properties=pika.BasicProperties(
                                       correlation_id = 11,
                                   )
                                               
    ranker.Rank(data, properties)
    
    query = {'first_name' : 'Binyamin',
             'last_name' : 'Netanyahu',
             'country' : 'Israel',
             'id' : '22829105'}
    
    fromDb(ranker, query, xrange(2900, 3000))
    '''
    
if __name__ == '__main__':
    main()