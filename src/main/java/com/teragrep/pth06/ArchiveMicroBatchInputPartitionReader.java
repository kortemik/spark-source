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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ArchiveMicroBatchInputPartitionReader implements InputPartitionReader<InternalRow> {
    final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroBatchInputPartitionReader.class);
    private long objectId;
    // this much has been consumed
    private AccumulatorV2<Long, Long> consumedAcc;
    // this much has been advertised
    private AccumulatorV2<Long, Long> lengthAcc;
    // this much can be consumed within this batch
    private long offsetFrom;
    private long offsetTo;
    private long currentOffset;



    public ArchiveMicroBatchInputPartitionReader(long objectId,
                                                 AccumulatorV2<Long, Long> consumedAcc,
                                                 AccumulatorV2<Long, Long> lengthAcc,
                                                 long offsetFrom,
                                                 long offsetTo) {
        System.out.println("ArchiveMicroBatchInputPartitionReader>");
        this.objectId = objectId;
        this.consumedAcc = consumedAcc;
        this.lengthAcc = lengthAcc;
        this.offsetFrom = offsetFrom;
        this.offsetTo = offsetTo;

        // initial status
        this.currentOffset = offsetFrom;

        // TODO connect to REST-01, pull the object with objectId, seek to offsetFrom and read until offsetTo
    }

    @Override
    public boolean next() throws IOException {
        System.out.println("ArchiveMicroBatchInputPartitionReader.next>");
        // check if position is at offsetTo and set to false
        if (currentOffset < offsetTo) {
            // TODO xxx testing, seems not to propagate?
            this.lengthAcc.add(9L);
            return true;
        } else {
            // bogus code for tests mock to indicated that we have one more, so another batch fires up
            this.lengthAcc.add(1L);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        System.out.println("ArchiveMicroBatchInputPartitionReader.get>");
        // TODO open the gzip file until maxOpenLimit has been reached'

        GenericInternalRow genericInternalRow = new GenericInternalRow(1);
        genericInternalRow.setLong(0, this.currentOffset);

        // inform driver about our progress
        this.consumedAcc.add(1L);
        this.currentOffset++;

        // TODO xxx testing, seems not to propagate?
        this.lengthAcc.add(9L);

        return genericInternalRow;
    }

    @Override
    public void close() throws IOException {
        System.out.println("ArchiveMicroBatchInputPartitionReader.close>");

    }
}
