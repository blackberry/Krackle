Krackle - A Low Overhead Kafka Client
=====================================

While the standard Java Kafka client is easy to use, it does tend to have a
high level of overhead.  A lot of objects are created, only to be garbage
collected very quickly, often within milliseconds on a heavily loaded 
producer.

Krackle is a Kafka 0.8 client designed to minimize the number of objects
created, and therefore to reduce garbage collection overhead.  In my tests
this has reduced the CPU usage by 50% under heavy load.

I order to achieve these performance improvements, some compromises had to be
made.  In particular, this producer requires an instance for each topic and 
key, and the consumer requires an instance per partition.  This means that it
may not useful in the general case.

Example use case:  You have thousands of applications running, and you are 
using Kafka to centralize the logs.  Since each application will know upfront
what topic it will log to, and the key it will use (a unique app instance id),
we can use the Krackle Producer effectively.  This means small savings
in CPU and memory usage on each instance, but that can add up over a large 
number of instances to provide large savings.

Basic Usage
-----------
```java
Properties props = ... // load some configuration properties
ProducerConfiguration conf = new ProducerConfiguration(props);
Producer producer = new Producer(conf, "clientId", "topic", "key");

// Use a byte buffer to store your data, and pass the data by referencing that.
byte[] buffer = new byte[1024];
while ( fillBuffer() ) { // Get some data in your buffer
    producer.send(buffer, offset, length);
}
producer.close();
```

```java
Properties props = ... // load some configuration properties
ConsumerConfiguration conf = new ConsumerConfiguration(props);
Consumer consumer = new Consumer(conf, "clientId", "topic", "key");

// Use a byte buffer to store the message you retrieve.
byte[] buffer = new byte[1024];
int bytesRead;
while ( true ) {
    bytesRead = consumer.getMessage(buffer, 0, buffer.length);
    if (bytesRead != -1) {
        // the first bytesRead bytes of the buffer are the message.
    }
}
consumer.close();
```

Producer Configuration
----------------------
Configuration is done via properties.  Many of these are the same as the
standard Java client.

<table>

<tr>
<th>property</th>
<th>default</th>
<th>description</th>
</tr>

<tr>
<td>metadata.broker.list</td>
<td></td>
<td>(required) A comma separated list of seed brokers to connect to in order
to get metadata about the cluster.</td>
</tr>

<tr>
<td>queue.buffering.max.ms</td>
<td>5000</td>
<td>Maximum time to buffer data. For example a setting of 100 will try to
batch together 100ms of messages to send at once. This will improve
throughput but adds message delivery latency due to the buffering.</td>
</tr>

<tr>
<td>request.required.acks</td>
<td>1</td>
<td>This value controls when a produce request is considered completed.
Specifically, how many other brokers must have committed the data to their
log and acknowledged this to the leader? Typical values are
<ul>
<li>0, which means that the producer never waits for an acknowledgement from
the broker (the same behavior as 0.7). This option provides the lowest
latency but the weakest durability guarantees (some data will be lost when a
server fails).</li>
<li>1, which means that the producer gets an acknowledgement after the leader
replica has received the data. This option provides better durability as the
client waits until the server acknowledges the request as successful (only
messages that were written to the now-dead leader but not yet replicated will
be lost).</li>
<li>-1, which means that the producer gets an acknowledgement after all
in-sync replicas have received the data. This option provides the best
durability, we guarantee that no messages will be lost as long as at least
one in sync replica remains.</li>
</ul>
</td>
</tr>

<tr>
<td>request.timeout.ms</td>
<td>10000</td>
<td>The amount of time the broker will wait trying to meet the
request.required.acks requirement before sending back an error to the client.
</td>
</tr>

<tr>
<td>message.send.max.retries</td>
<td>3</td>
<td>This property will cause the producer to automatically retry a failed
send request. This property specifies the number of retries when such
failures occur. Note that setting a non-zero value here can lead to
duplicates in the case of network errors that cause a message to be sent but
the acknowledgement to be lost.</td>
</tr>

<tr>
<td>retry.backoff.ms</td>
<td>100</td>
<td>Before each retry, the producer refreshes the metadata of relevant topics
to see if a new leader has been elected. Since leader election takes a bit of
time, this property specifies the amount of time that the producer waits
before refreshing the metadata.</td>
</tr>

<tr>
<td>topic.metadata.refresh.interval.ms</td>
<td>600 * 1000</td>
<td>The producer generally refreshes the topic metadata from brokers when
there is a failure (partition missing, leader not available...). It will also
poll regularly (default: every 10min so 600000ms). If you set this to a
negative value, metadata will only get refreshed on failure. If you set this
to zero, the metadata will get refreshed after each message sent (not
recommended). Important note: the refresh happen only AFTER the message is
sent, so if the producer never sends a message the metadata is never
refreshed</td>
</tr>

<tr>
<td>message.buffer.size</td>
<td>1024*1024</td>
<td>The size of each buffer that is used to store raw messages before they
are sent. Since a full buffer is sent at once, don't make this too big.</td>
</tr>

<tr>
<td>num.buffers</td>
<td>2</td>
<td>The number of buffers to use. At any given time, there is up to one
buffer being filled with new data, up to one buffer having its data sent to
the broker, and any number of buffers waiting to be filled and/or sent.

Essentially, the limit of the amount of data that can be queued at at any
given time is message.buffer.size * num.buffers. Although, in reality, you
won't get buffers to 100% full each time.</td>
</tr>

<tr>
<td>send.buffer.size</td>
<td>message.buffer.size + 200</td>
<td>Size of the byte buffer used to store the final (with headers and
compression applied) data to be sent to the broker.</td>
</tr>

<tr>
<td>compression.codec</td>
<td>none</td>
<td>This parameter allows you to specify the compression codec for all data
generated by this producer. Valid values are "none", "gzip" and "snappy".</td>
</tr>

<tr>
<td>gzip.compression.level</td>
<td>java.util.zip.Deflater.DEFAULT_COMPRESSION</td>
<td>If compression.codec is set to gzip, then this allows configuration of
the compression level.
<ul>
<li>-1: default compression level</li>
<li>0: no compression</li>
<li>1-9: 1=fastest compression ... 9=best compression</li>
</ul>
</td>
</tr>

<tr>
<td>queue.enqueue.timeout.ms</td>
<td>-1</td>
<td>The amount of time to block before dropping messages when all buffers are
full. If set to 0 events will be enqueued immediately or dropped if the queue
is full (the producer send call will never block). If set to -1 the producer
will block indefinitely and never willingly drop a send.</td>
</tr>

</table>

Consumer Configuration
----------------------
Configuration is done via properties.  Many of these are the same as the
standard Java client.

<table>
<tr>
<th>property</th>
<th>default</th>
<th>description</th>
</tr>

<tr>
<td>metadata.broker.list</td>
<td></td>
<td>(required) A list of seed brokers to connect to in order to get
information about the Kafka broker cluster.</td>
</tr>

<tr>
<td>fetch.message.max.bytes</td>
<td>1024 * 1024</td>
<td>The number of byes of messages to attempt to fetch for each
topic-partition in each fetch request. These bytes will be read into memory
for each partition, so this helps control the memory used by the consumer.
The fetch request size must be at least as large as the maximum message size
the server allows or else it is possible for the producer to send messages
larger than the consumer can fetch.</td>
</tr>

<tr>
<td>fetch.wait.max.ms</td>
<td>100</td>
<td>The maximum amount of time the server will block before answering the
fetch request if there isn't sufficient data to immediately satisfy
fetch.min.bytes</td>
</tr>

<tr>
<td>fetch.min.bytes</td>
<td>1</td>
<td>The minimum amount of data the server should return for a fetch request.
If insufficient data is available the request will wait for that much data to
accumulate before answering the request.</td>
</tr>

<tr>
<td>socket.receive.buffer.bytes</td>
<td>64 * 1024</td>
<td>The socket receive buffer for network requests</td>
</tr>

<tr>
<td>auto.offset.reset</td>
<td>largest</td>
<td>What to do when there is no initial offset in ZooKeeper or if an offset
is out of range:
<ul>
<li>smallest : automatically reset the offset to the smallest offset</li>
<li>largest : automatically reset the offset to the largest offset</li>
<li>anything else: throw exception to the consumer</li>
</ul>
</td>
</tr>
</table>

## Contributing
To contribute code to this repository you must be [signed up as an official contributor](http://blackberry.github.com/howToContribute.html).

## Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.