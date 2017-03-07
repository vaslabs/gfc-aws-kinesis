# gfc-aws-kinesis [![Join the chat at https://gitter.im/gilt/gfc](https://badges.gitter.im/gilt/gfc.svg)](https://gitter.im/gilt/gfc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Scala wrapper around AWS Kinesis Client Library. Part of the [Gilt Foundation Classes](https://github.com/gilt?q=gfc).

## Getting gfc-aws-kinesis

The latest version is 0.11.0, which is cross-built against Scala 2.11.x and 2.12.x.

SBT dependency:

```scala
libraryDependencies += "com.gilt" %% "gfc-aws-kinesis" % "0.11.0"
```

SBT Akka stream dependency:

```scala
libraryDependencies += "com.gilt" %% "gfc-aws-kinesis-akka" % "0.11.0"
```

# Basic usage

Consume events:

```scala

  implicit object StringRecordReader extends KinesisRecordReader[String]{
    override def apply(r: Record) : String = new String(r.data.array(), "UTF-8")
  }

  val config = KCLConfiguration("consumer-name", "kinesis-stream-name")

  KCLWorkerRunner(config).runAsyncSingleRecordProcessor[A](1 minute) { a: String =>
     // .. do something with A
  }
```

Publish events:

```scala

  implicit object StringRecordWriter extends KinesisRecordWriter[String] {
    override def toKinesisRecord(a: String) : KinesisRecord = {
      KinesisRecord("partition-key", a.getBytes("UTF-8"))
    }
  }

  val publisher = KinesisPublisher()

  val messages = Seq("Hello World!", "foo bar", "baz bam")

  val result: Future[KinesisPublisherBatchResult] = publisher.publishBatch("kinesis-stream-name", messages)
```
