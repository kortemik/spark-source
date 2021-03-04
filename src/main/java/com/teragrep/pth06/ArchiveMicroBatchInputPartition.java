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
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;

class ArchiveMicroBatchInputPartition implements InputPartition<InternalRow> {
    private long objectId;
    private AccumulatorV2<Long, Long> consumedAcc;
    private AccumulatorV2<Long, Long> lengthAcc;
    private String preferredLocation;
    private long offsetFrom;
    private long offsetTo;

    public ArchiveMicroBatchInputPartition(long objectId,
                                           AccumulatorV2<Long, Long> consumedAcc,
                                           AccumulatorV2<Long, Long> lengthAcc,
                                           String preferredLocation,
                                           long offsetFrom,
                                           long offsetTo) {
        System.out.println("ArchiveMicroBatchInputPartition>");
        this.objectId = objectId;
        this.consumedAcc = consumedAcc;
        this.lengthAcc = lengthAcc;
        this.preferredLocation = preferredLocation;
        this.offsetFrom = offsetFrom;
        this.offsetTo = offsetTo;

        // TODO xxx tests
        this.lengthAcc.add(200L);
    }

    @Override
    public String[] preferredLocations() {
        System.out.println("ArchiveMicroBatchInputPartition.preferredLocations>");
        String[] loc = new String[1];
        loc[0] = this.preferredLocation;
        return loc;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        System.out.println("ArchiveMicroBatchInputPartition.createPartitionReader>");
        // TODO xxx tests
        this.lengthAcc.add(50L);

        return new ArchiveMicroBatchInputPartitionReader(
                this.objectId,
                this.consumedAcc,
                this.lengthAcc,
                this.offsetFrom,
                this.offsetTo);
    }
}
