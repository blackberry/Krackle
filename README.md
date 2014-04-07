Kafka Low Overhead Producer
===========================

While the standard Java Kafka Producer is easy to use, it does tend to have a
high level of overhead.  A lot of objects are created, only to be garbage
collected very quickly, often within milliseconds on a heavily loaded 
producer.

This is a Kafka 0.8 Producer designed to minimize the number of objects
created, and therefore to reduce garbage collection overhead.  In my tests
this has reduced the CPU usage by 50% under heavy load.

I order to achive these performance improvements, some compromises had to be
made.  In particular, this producer requires an instance for each topic and 
key.  This means that it's not useful in the general case.

Example use case:  You have thousands of applications running, and you are 
using Kafka to centralize the logs.  Since each application will know upfront
what topic it will log to, and the key it will use (a unique app instance id),
we can use the Low Overhead Producer effectively.  This means small savings
in CPU and memory usage on each instance, but that can add up over a large 
number of instances to provice large savings.

Basic Usage
-----------
```java
Properties props = ... // load some configuration properties
Configuration conf = new Configuration(props);
LowOverHeadProducer producer = new LowOverheadProducer(conf, "clientId", "topic", "key");

// Use a byte buffer to store your data, and pass the data by referencing that.
byte[] buffer = new byte[1024];
while ( fillBuffer() ) { // Get some data in your buffer
  producer.send(buffer, offset, length);
}
producer.close();
```

Configuration
-------------
Configuration is done via properties.  Many of these are the same as the
standard Java client.

| Description                    | Default | Description |
|--------------------------------|---------|-------------|
| metadata.broker.list           |         |             |
| requrest.required.acks         |         |             |
| request.timeout.ms             |         |             |
| message.send.max.retries       |         |             |
| retry.backoff.ms               |         |             |
| compression.codec              |         |             |
| message.buffer.size            |         |             |
| send.buffer.size               |         |             |
| response.buffer.size           |         |             |
| compression.level              |         |             |
| metrics.to.console             |         |             |
| metrics.to.console.interval.ms |         |             |
