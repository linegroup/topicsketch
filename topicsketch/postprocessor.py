__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

import experiment.exp_config as config

_THRESHOLD_FOR_SIMILARITY = eval(config.get('post_process', 'threshold_for_similarity'))
_THRESHOLD_TOPIC_LEVEL_NUMBER_RELATED_USERS = eval(config.get('post_process', 'topic_number_related_users'))
_THRESHOLD_TOPIC_LEVEL_NUMBER_RELATED_TWEETS = eval(config.get('post_process', 'topic_number_related_tweets'))
_THRESHOLD_WORD_LEVEL_NUMBER_RELATED_USERS = eval(config.get('post_process', 'word_number_related_users'))
_THRESHOLD_WORD_LEVEL_NUMBER_RELATED_TWEETS = eval(config.get('post_process', 'word_number_related_tweets'))


def similarity(set_words, tokens):
    s = 0.

    for token in tokens:
        if token in set_words:
            s += 1

    return s


def process(high_prob_words, active_terms):  # high_prob_words and active_terms from topic_sketch

    set_words = set()
    for prob_word in high_prob_words:
        set_words.add(prob_word[0])

    # for topic level
    n_related_tweets = 0
    set_users = set()

    # for word level
    dict_tweets = dict()
    dict_users = dict()

    for active_term in active_terms:
        tokens = active_term[1]
        uid = active_term[2]
        s = similarity(set_words, tokens)
        if s >= _THRESHOLD_FOR_SIMILARITY:
            n_related_tweets += 1
            set_users.add(uid)

            for token in tokens:
                if token in set_words:
                    if token not in dict_tweets:
                        dict_tweets[token] = 0
                    dict_tweets[token] += 1
                    if token not in dict_users:
                        dict_users[token] = set()
                    dict_users[token].add(uid)

    n_related_users = len(set_users)

    word_level_results = list()
    for prob_word in high_prob_words:
        _word = prob_word[0]
        word_level_result = False

        if _word in dict_tweets and _word in dict_users:

            if dict_tweets[_word] >= _THRESHOLD_WORD_LEVEL_NUMBER_RELATED_TWEETS and \
                            len(dict_users[_word]) >= _THRESHOLD_WORD_LEVEL_NUMBER_RELATED_USERS:
                word_level_result = True

        word_level_results.append(word_level_result)

    if n_related_tweets >= _THRESHOLD_TOPIC_LEVEL_NUMBER_RELATED_TWEETS and n_related_users >= _THRESHOLD_TOPIC_LEVEL_NUMBER_RELATED_USERS:
        print 'related: ' + str(n_related_tweets) + ',' + str(n_related_users)  # debugging
        return True, word_level_results, {'n_related_tweets': n_related_tweets, 'n_related_users':n_related_users}

    return False, word_level_results, {'n_related_tweets': n_related_tweets, 'n_related_users':n_related_users}
