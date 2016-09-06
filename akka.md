com.gilt.gfc.aws.kinesis.akka is a library to create akka stream Source out of Kinesis stream

# Examples:

For already materialized stream

```scala
    val flow = Source.queue[MyRecordType](0, OverflowStrategy.backpressure)
      .map(x => s"Flow got message $x")
      .to(Sink.foreach(println))
      .run()
      
    val streamConfig = KinesisStreamConsumerConfig[MyRecordType](
      "my-test-stream",
      "kinesis-consumer-service-1",
      bytes => JsonParser(bytes).convertTo[MyRecordType]
    )
    
    val consumer = new KinesisStreamConsumer[MyRecordType](
      streamConfig,
      KinesisStreamHandler(KinesisStreamSource.pumpKinesisStreamTo(flow, 10.second))
    )

    val ec = Executors.newSingleThreadExecutor()
    ec.submit(new Runnable {
      override def run(): Unit = consumer.run()
    })
```

For not yet materialized stream

```scala
    val flow = Flow[MyRecordType]
      .map(x => s"Flow got message $x")
      .to(Sink.foreach(println))

    val streamConfig = KinesisStreamConsumerConfig[MyRecordType](
      "my-test-stream",
      "kinesis-consumer-service-2",
      bytes => JsonParser(bytes).convertTo[MyRecordType]
    )
    
    val kinesisSource = KinesisStreamSource(
      streamConfig,
      10.second
    )

    slowFlow.runWith(kinesisSource)

```
