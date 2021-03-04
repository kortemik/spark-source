# spark-source

```
sc.version
res1: String = 2.4.0-cdh6.2.0
```

Driver logs
```
ArchiveMicroBatchReader>
ArchiveMicroBatchReader.readSchema>
ArchiveMicroBatchReader.stop>
ArchiveMicroBatchReader>
ArchiveMicroBatchReader.setOffsetRange>
ArchiveMicroBatchReader.setOffsetRange> start: null
ArchiveMicroBatchReader.setOffsetRange> end: null
ArchiveMicroBatchReader.getEndOffset>
ArchiveMicroBatchReader.getEndOffset> return {0=1, 1=1}
ArchiveMicroBatchReader.setOffsetRange>
ArchiveMicroBatchReader.setOffsetRange> start: null
ArchiveMicroBatchReader.setOffsetRange> end: Optional[{"0":1,"1":1}]
ArchiveMicroBatchReader.readSchema>
ArchiveMicroBatchReader.planInputPartitions>
ArchiveMicroBatchInputPartition>
ArchiveMicroBatchInputPartition>
ArchiveMicroBatchInputPartition.preferredLocations>
ArchiveMicroBatchInputPartition.preferredLocations>
ArchiveMicroBatchReader.deserializeOffset> {"0":1,"1":1}
ArchiveMicroBatchReader.setOffsetRange>
ArchiveMicroBatchReader.setOffsetRange> start: Optional[{"0":1,"1":1}]
ArchiveMicroBatchReader.setOffsetRange> end: null
ArchiveMicroBatchReader.setOffsetRange> end of 0 update: 201
ArchiveMicroBatchReader.setOffsetRange> end of 1 update: 201
ArchiveMicroBatchReader.getEndOffset>
ArchiveMicroBatchReader.getEndOffset> return {0=201, 1=201}
ArchiveMicroBatchReader.deserializeOffset> {"0":201,"1":201}
ArchiveMicroBatchReader.setOffsetRange>
ArchiveMicroBatchReader.setOffsetRange> start: Optional[{"0":201,"1":201}]
ArchiveMicroBatchReader.setOffsetRange> end: null
ArchiveMicroBatchReader.setOffsetRange> end of 0 update: 201
ArchiveMicroBatchReader.setOffsetRange> end of 1 update: 201
ArchiveMicroBatchReader.getEndOffset>
ArchiveMicroBatchReader.getEndOffset> return {0=201, 1=201}
ArchiveMicroBatchReader.deserializeOffset> {"0":201,"1":201}
ArchiveMicroBatchReader.setOffsetRange>
ArchiveMicroBatchReader.setOffsetRange> start: Optional[{"0":201,"1":201}]
ArchiveMicroBatchReader.setOffsetRange> end: null
ArchiveMicroBatchReader.setOffsetRange> end of 0 update: 201
ArchiveMicroBatchReader.setOffsetRange> end of 1 update: 201
ArchiveMicroBatchReader.getEndOffset>
ArchiveMicroBatchReader.getEndOffset> return {0=201, 1=201}
... (repeats)
```

Task logs
```
ArchiveMicroBatchInputPartition.createPartitionReader>
ArchiveMicroBatchInputPartitionReader>
ArchiveMicroBatchInputPartitionReader.next>
ArchiveMicroBatchInputPartitionReader.get>
ArchiveMicroBatchInputPartitionReader.next>
ArchiveMicroBatchInputPartitionReader.close>
```
