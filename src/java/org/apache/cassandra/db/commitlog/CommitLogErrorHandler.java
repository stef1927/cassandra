package org.apache.cassandra.db.commitlog;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Killer;

@VisibleForTesting
public final class CommitLogErrorHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogErrorHandler.class);

    public static boolean handleCommitError(String message, Throwable t)
    {
        logger.info("Commit failure policy {}, message {}, t {}", DatabaseDescriptor.getCommitFailurePolicy(), message, t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
        case die:
            Killer.kill(t);
        case stop:
            StorageService.instance.stopTransports();
        case stop_commit:
            logger.error(String.format("%s. Commit disk failure policy is %s; terminating thread", message, DatabaseDescriptor.getCommitFailurePolicy()),
                                   t);
            return false;
        case ignore:
            logger.error(message, t);
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            return true;
        default:
            throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }
}
