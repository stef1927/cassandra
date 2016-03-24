/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A class for bulk read of local data, see CASSANDRA-9259.
 */
public class BulkReadService
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private final SelectStatement statement;

    public BulkReadService(SelectStatement statement)
    {
        this.statement = statement;
    }

    public ResultMessage.Rows execute(ReadQuery query,
                                      QueryOptions options,
                                      QueryState state,
                                      int nowInSec,
                                      int pageSize)
        throws RequestExecutionException, RequestValidationException
    {
        try
        {
            CompletableFuture<ResultMessage.Rows> firstMessage = new CompletableFuture<>();
            StageManager.getStage(Stage.READ).execute(new ReadRunnable(firstMessage, statement, query, options, state, nowInSec, pageSize));
            return firstMessage.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final static class ReadRunnable implements Runnable
    {
        private final CompletableFuture<ResultMessage.Rows> firstMessage;
        private final SelectStatement statement;
        private final ReadQuery query;
        private final QueryOptions options;
        private final QueryState state;
        private final int nowInSec;
        private final int pageSize;
        private final long start;

        ReadRunnable(CompletableFuture<ResultMessage.Rows> firstMessage,
                     SelectStatement statement,
                     ReadQuery query,
                     QueryOptions options,
                     QueryState state,
                     int nowInSec,
                     int pageSize)
        {
            this.firstMessage = firstMessage;
            this.statement = statement;
            this.query = query;
            this.options = options;
            this.state = state;
            this.nowInSec = nowInSec;
            this.pageSize = pageSize;
            this.start = System.nanoTime();
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        protected void runMayThrow()
        {
            try
            {
                ResultSetStreamer streamer = new ResultSetStreamer(this);

                try (ReadExecutionController executionController = query.executionController())
                {
                    try (PartitionIterator it = query.executeInternal(executionController))
                    {

                        process(Transformation.apply(it, streamer), streamer);
                    }
                }

                streamer.sendCurrent();
            }
            catch (Throwable t)
            {
                if (t instanceof TombstoneOverwhelmingException)
                    logger.error(t.getMessage());
                else
                    throw t;
            }
        }

        private void process(PartitionIterator partitions, ResultSetStreamer streamer) throws InvalidRequestException
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    statement.processPartition(partition, options, streamer, nowInSec);
                }
            }
        }
    }

    private static final class ResultSetStreamer extends Transformation<RowIterator> implements Selection.ResultSetAccumulator
    {
        private final CompletableFuture<ResultMessage.Rows> firstMessage;
        private final SelectStatement statement;
        private final QueryOptions options;
        private final QueryState state;
        private final DataLimits pageLimits;

        private boolean completed = false;
        private int remaining;
        private Selection.ResultSetBuilder builder;

        public ResultSetStreamer(ReadRunnable parent)
        {
            this.firstMessage = parent.firstMessage;
            this.statement = parent.statement;
            this.options = parent.options;
            this.state = parent.state;
            this.pageLimits = parent.query.limits().forPaging(parent.pageSize);

            nextPage();
        }

        private void nextPage()
        {
            this.remaining = pageLimits.count();
            this.builder = statement.selection.resultSetBuilder(statement.parameters.isJson);
        }

        public void add(ByteBuffer v)
        {
            builder.add(v);
        }

        public void add(Cell c, int nowInSec)
        {
            builder.add(c, nowInSec);
        }

        public void newRow(int protocolVersion) throws InvalidRequestException
        {
            if (remaining == 0)
            {
                sendCurrent();
                nextPage();
            }

            builder.newRow(protocolVersion);
            remaining--;
        }

        @Override
        public void onClose()
        {
            completed = true;
        }

        private void sendCurrent()
        {
            ResultSet cqlRows = builder.build(options.getProtocolVersion());
            statement.orderResults(cqlRows);

            ResultMessage.Rows response = new ResultMessage.Rows(cqlRows);
            if (!completed)
                response.setMultiPart();

            if (!firstMessage.isDone())
            {
                firstMessage.complete(response);
            }
            else
            {
                response.prepareForSending(state.getStreamId(), state.getConnection());
                state.getConnection().channel().writeAndFlush(response);
            }
        }
    }
}
