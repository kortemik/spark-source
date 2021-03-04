/*
 * PTH-06
 * Copyright (C) 2021  Suomen Kanuuna Oy
 *  
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *  
 *  
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *  
 * If you modify this Program, or any covered work, by linking or combining it 
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *  
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *  
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *  
 * Names of the licensors and authors may not be used for publicity purposes.
 *  
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *  
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *  
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.pth06;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.util.*;

class ArchiveMicroBatchReader implements MicroBatchReader {
    final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroBatchReader.class);

    private Option<SparkSession> spark;
    private String metadataPath = null;


    // object status accumulators
    // objectId, reader positions
    private Map<Long, AccumulatorV2<Long, Long>> offsetsConsumed;
    // objectId, reader sneak peek position
    private Map<Long, AccumulatorV2<Long, Long>> offsetsAvailable;


    // requested offsets as Map[ObjectId (Long), Offset within(Long)]
    // these are passed back to the Job, and synchronized from the accumulators
    private TreeMap<Long, Long> startOffsetMap = null;
    private TreeMap<Long, Long> endOffsetMap = null;

    ArchiveMicroBatchReader() {
        System.out.println("ArchiveMicroBatchReader>");
        // these two are maintained for the job, internally accumulators are used
        // object id, counters
        this.startOffsetMap = new TreeMap<Long, Long>();
        this.endOffsetMap = new TreeMap<Long, Long>();

        // object offset states, objectId, counters
        this.offsetsConsumed = new HashMap<Long, AccumulatorV2<Long, Long>>();
        this.offsetsAvailable = new HashMap<Long, AccumulatorV2<Long, Long>>();

        this.spark = SparkSession.getActiveSession();

        // TODO initialize and connect REST-01 API connector
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        System.out.println("ArchiveMicroBatchReader.setOffsetRange>");
        // TODO fetch objects
        // this bogus code fakes only objectId's 1 and 2

        if (!start.isPresent()) {
            System.out.println("ArchiveMicroBatchReader.setOffsetRange> start: null");
            // initial values
            // initialize consumed accumulators
            Long[] objectIds = {0L, 1L};
            for (long id: objectIds) {
                AccumulatorV2<Long, Long> offsetConAcc = spark.get().sparkContext().longAccumulator();
                this.offsetsConsumed.put(id, offsetConAcc);
                this.startOffsetMap.put(id, offsetConAcc.value());
            }
        } else {
            System.out.println("ArchiveMicroBatchReader.setOffsetRange> start: " + start.toString());
            // TODO set
        }

        if (!end.isPresent()) {
            System.out.println("ArchiveMicroBatchReader.setOffsetRange> end: null");
            // accumulators for found objects
            // available
            Long[] objectIds = {0L, 1L};
            for (long id: objectIds) {
                if (!offsetsAvailable.containsKey(id)) {
                    // initial offsets are 1L to open the gzip file
                    AccumulatorV2<Long, Long> offsetAvailAcc = spark.get().sparkContext().longAccumulator();
                    offsetAvailAcc.add(1L); // initial
                    this.offsetsAvailable.put(id, offsetAvailAcc);
                    this.endOffsetMap.put(id, offsetAvailAcc.value());
                }
                else {
                    // update end
                    System.out.println("ArchiveMicroBatchReader.setOffsetRange> end of " + id +
                            " update: "+ this.offsetsAvailable.get(id).value());
                    this.endOffsetMap.put(id, this.offsetsAvailable.get(id).value());
                }
            }
        } else {
            System.out.println("ArchiveMicroBatchReader.setOffsetRange> end: " + end.toString());
            // TODO set
        }
    }

    @Override
    public Offset getStartOffset() {
        System.out.println("ArchiveMicroBatchReader.getStartOffset>");
        System.out.println("ArchiveMicroBatchReader.getStartOffset> return " + this.startOffsetMap.toString());
        return new ArchiveSourceOffset(this.startOffsetMap);
    }

    @Override
    public Offset getEndOffset() {
        System.out.println("ArchiveMicroBatchReader.getEndOffset>");
        System.out.println("ArchiveMicroBatchReader.getEndOffset> return " + this.endOffsetMap.toString());
        return new ArchiveSourceOffset(this.endOffsetMap);
    }

    @Override
    public Offset deserializeOffset(String s) {
        System.out.println("ArchiveMicroBatchReader.deserializeOffset> " + s.toString());
        try {
            return new ArchiveSourceOffset(s);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void commit(Offset offset) {
        System.out.println("ArchiveMicroBatchReader.commit> " + offset.toString());
        // TODO remove accumulators for the ones that have been committed fully

    }

    @Override
    public void stop() {
        System.out.println("ArchiveMicroBatchReader.stop>");
        // TODO shutdown the REST-01 API connector
    }

    @Override
    public StructType readSchema() {
        System.out.println("ArchiveMicroBatchReader.readSchema>");
        // real one commented out for tests
        //return ArchiveDataSource.Schema;
        return new StructType(
                new StructField[]{
                        new StructField("eventId", DataTypes.LongType, true, new MetadataBuilder().build())
                }
        );
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        System.out.println("ArchiveMicroBatchReader.planInputPartitions>");
        Option<SparkSession> spark = SparkSession.getActiveSession();
        if (!spark.isDefined()) {
            return null;
        }

        AccumulatorV2<Long, Long> asd = spark.get().sparkContext().longAccumulator();
        asd.value();
        List<InputPartition<InternalRow>> inputPartitions = new LinkedList<InputPartition<InternalRow>>();
        // for i in options.get(NUM_PARTITIONS) or activeSession.get.sparkContext.defaultParallelism; do
        inputPartitions.add(new ArchiveMicroBatchInputPartition(0L,
                        this.offsetsConsumed.get(0L),
                        this.offsetsAvailable.get(0L),
                        "here.domain.tld",
                        this.startOffsetMap.get(0L),
                        this.endOffsetMap.get(0L)
                )
        );
        inputPartitions.add(new ArchiveMicroBatchInputPartition(1L,
                        this.offsetsConsumed.get(1L),
                        this.offsetsAvailable.get(1L),
                        "there.domain.tld",
                        this.startOffsetMap.get(1L),
                        this.endOffsetMap.get(1L)
                )
        );
        return inputPartitions;
    }
}
