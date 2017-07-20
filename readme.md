# TopicSketch

This is a simple implementation of the paper "TopicSketch: Real-Time Bursty Topic Detection from Twitter" http://ieeexplore.ieee.org/document/7457245/?reload=true&arnumber=7457245&filter%3DAND(p_IS_Number:7505473)

This project is mainly coded using python, while for the reason of efficiency, some core parts are implemented using C and Cython.


## Getting Started

### Data Flow
The data flow is as follows. (See main.py.)

stream ==> preprocessor ==> detection component ==> topicsketch ==> detected topic


### Stream
In order to run TopicSketch, a stream class is needed. It should extend topicsketch.stream.ItemStream . Everytime, TopicSketch will call stream.next() to get the next tweet.
Each tweet is represented as a tuple (timestamp, user_id, tweet_text). experiment.tweet_stream is an example, which get tweets from a MySQL database.

Notice: make sure the tweet stream in the order of time !!!


### Tokenization
In this project, I use twokenize (https://github.com/myleott/ark-twokenize-py) for tokenization. For different languages, other tokenizer may be used.


### Compile
For hashing

hash.c is an implementation of the multi-linear family hash function.

cd c; gcc -fPIC -shared -o mlh.so  hash.c


For acceleration and significance score

acceleration is defined in the above paper "TopicSketch".

significance score is defined in the paper "SigniTrend: scalable detection of emerging topics in textual streams by hashed significance thresholds".

install Cython first: pip install cython

cd cython

cd fast_smoother; python setup.py build_ext --inplace

cd fast_signi; python setup.py build_ext --inplace

After compiling, copy the libraries into folder TopicSketch.

### Stop Words

Put stop words in twitter-stopwords.txt. Define your own stop words file according to your data set.

### Parameters
Set parameters in file "parameters.cnf". At least set start_t and end_t under section [detection].

### Memory
It is suggested to use a machine which has enough memory (>=16GB).


### Detection Threshold
It is suggested to set high threshold for reliable results.

### Spam
It is suggested to remove spam accounts from the data set before running the detection algorithm.


### Scipy
Update scipy to the latest version for best computational performance.


### Running the Program
Once you implement your stream class, replace tweet_stream in main.py by your stream. Then run main.py. Debugging information will appear in the CLI. Detected topics will be saved in file results.txt.
