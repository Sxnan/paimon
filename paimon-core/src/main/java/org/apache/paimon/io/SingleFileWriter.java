package org.apache.paimon.io;

import org.apache.paimon.fs.Path;

import java.io.IOException;

/**
 * File writer to accept one record or a bunch of records, write to a single logical file and generate
 * metadata after closing it.
 *
 * @param <T> record type.
 * @param <R> file result to collect.
 */
// TODO: Proper naming of the class
public interface SingleFileWriter<T, R> extends FileWriter<T, R> {

    void writeBundle(BundleRecords bundle) throws IOException;

    boolean reachTargetSize(boolean b, long targetFileSize) throws IOException;

    Path path();

    AbortExecutor abortExecutor();

    interface AbortExecutor {
        void abort();
    }
}
