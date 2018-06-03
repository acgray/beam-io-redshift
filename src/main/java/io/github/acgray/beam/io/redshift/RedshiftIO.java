package io.github.acgray.beam.io.redshift;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({ "unused", "SameParameterValue" })
public class RedshiftIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedshiftIO.class);

  public static <T> Write<T> write() {
    return new AutoValue_RedshiftIO_Write.Builder<T>()
      .setIntermediateBucketName(null)
      .setIntermediateS3Prefix(null)
      .setDenormalizingFunction(null)
      .setCompression(Compression.GZIP)
      .setCopyStatementPerFile(false)
      .build();
  }

  @AutoValue
  public abstract static class Write<T>
    extends PTransform<PCollection<T>, PDone> {

    public Write<T> withTargetConfiguration(
      TargetConfiguration configuration) {
      return this.toBuilder()
        .setTargetConfiguration(configuration)
        .build();
    }

    public Write<T> toTable(String tableName) {
      return this.toBuilder()
        .setTableName(tableName)
        .build();
    }

    public Write<T> withIntermediateBucketName(String intermediateBucketName) {
      return this.toBuilder()
        .setIntermediateBucketName(intermediateBucketName)
        .build();
    }

    public Write<T> withDenormalizingFunction(
      SerializableFunction<T, List<String>> denormalizingFunction) {
      return this.toBuilder()
        .setDenormalizingFunction(denormalizingFunction)
        .build();
    }

    public Write<T> withS3Prefix(String s3Prefix) {
      return this.toBuilder()
        .setIntermediateS3Prefix(s3Prefix)
        .build();
    }

    public Write<T> withAwsCredentials(String credentials) {
      return this.toBuilder()
        .setCredentials(credentials)
        .build();
    }

    public Write<T> withCopyStatementPerFile() {
      return this.toBuilder()
        .setCopyStatementPerFile(true)
        .build();
    }

    @Nullable
    abstract SerializableFunction<T, List<String>> getDenormalizingFunction();

    @Nullable
    abstract String getIntermediateBucketName();

    @Nullable
    abstract String getIntermediateS3Prefix();

    @Nullable
    abstract TargetConfiguration getTargetConfiguration();

    @Nullable
    abstract String getTableName();

    @Nullable
    abstract String getCredentials();

    @Nullable
    abstract Compression getCompression();

    abstract boolean getCopyStatementPerFile();

    abstract Builder<T> toBuilder();

    private String jdbcUrl() {
      return "";
    }

    private static class CSVSink implements FileIO.Sink<List<String>> {

      transient Writer writer;
      transient CSVPrinter printer;

      @Override
      public void open(WritableByteChannel channel) throws IOException {
        this.writer = Channels.newWriter(channel, "utf-8");
        this.printer = new CSVPrinter(this.writer, CSVFormat.INFORMIX_UNLOAD);
      }

      @Override
      public void write(List<String> element) throws IOException {
        this.printer.printRecord(element);
      }

      @Override
      public void flush() throws IOException {
        this.printer.flush();
      }
    }

    private static class ExecuteCopyFn extends DoFn<String, Void> {

      String tableName;
      TargetConfiguration targetConfiguration;
      Compression compression;
      String credentials;
      String bucketName;

      transient Connection cxn;

      static final String COPY_STATEMENT_TEMPLATE = ""
        + "COPY %s FROM 's3://%s/%s'\n"
        + "CREDENTIALS AS '%s'\n"
        + "DELIMITER AS '|'\n"
        + "REMOVEQUOTES\n"
        + "ESCAPE\n"
        + "%s\n";

      ExecuteCopyFn(
        String tableName,
        TargetConfiguration targetConfiguration,
        Compression compression,
        String credentials,
        String bucketName
      ) {
        this.tableName = tableName;
        this.targetConfiguration = targetConfiguration;
        this.compression = compression;
        this.credentials = credentials;
        this.bucketName = bucketName;
      }

      @Setup
      public void setup() throws Exception {

        LOG.info("Opening connection to {}",
          this.targetConfiguration.getJdbcUrl());

        this.cxn = this.targetConfiguration
          .buildDataSource()
          .getConnection();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String path = context.element();
        LOG.info("Processing {}", path);

        String sql = String.format(
          COPY_STATEMENT_TEMPLATE,
          tableName,
          bucketName,
          path,
          credentials,
          compression.equals(Compression.UNCOMPRESSED) ?
            "" :
            compression.name());

        LOG.info("Executing {}", sql);

        Statement stmt = this.cxn.createStatement();

        stmt.execute(sql);

      }

      @Teardown
      public void teardown() throws Exception {
        LOG.info("Closing connection");
        cxn.close();
      }

    }

    @Override
    public PDone expand(PCollection<T> input) {

      Objects.requireNonNull(getDenormalizingFunction());
      Objects.requireNonNull(
        getIntermediateBucketName(),
        "intermediateBucketName must be provided");
      Objects.requireNonNull(
        getCredentials(),
        "credentials must be provided");
      Objects.requireNonNull(
        getTableName(),
        "tableName must be provided");
      Preconditions.checkArgument(
        Arrays.asList(
          Compression.UNCOMPRESSED,
          Compression.GZIP,
          Compression.BZIP2).contains(getCompression()),
        "Only gzip, bzip2 and uncompressed input is supported");

      WriteFilesResult<Void> result = input.apply(
        MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
          .via(getDenormalizingFunction()))

        .apply(FileIO.<List<String>>write()
          .to(String.format(
            "s3://%s/%s/",
            getIntermediateBucketName(),
            getIntermediateS3Prefix()))
          .withCompression(getCompression())
          .via(new CSVSink()));

      PCollection<String> files = result
        .getPerDestinationOutputFilenames()
        .apply(Values.create());

      if (getCopyStatementPerFile()) {

        files.apply(ParDo.of(new ExecuteCopyFn(
          getTableName(),
          getTargetConfiguration(),
          getCompression(),
          getCredentials(),
          getIntermediateBucketName()
        )));
      } else {
        files.apply(MapElements.into(TypeDescriptors.strings())
          .via(e -> Utils.coalesce(getIntermediateS3Prefix(), "")))
          .apply(Count.perElement())
          .apply(Keys.create())
          .apply(ParDo.of(
            new ExecuteCopyFn(
              getTableName(),
              getTargetConfiguration(),
              getCompression(),
              getCredentials(),
              getIntermediateBucketName()
            )
          ));
      }

      return PDone.in(input.getPipeline());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setIntermediateBucketName(
        String intermediateBucketName);

      abstract Builder<T> setDenormalizingFunction(
        SerializableFunction<T, List<String>> denormalizingFunction);

      abstract Builder<T> setTableName(String tableName);

      abstract Builder<T> setIntermediateS3Prefix(String intermediateS3Prefix);

      abstract Builder<T> setCredentials(String credentials);

      abstract Builder<T> setTargetConfiguration(
        TargetConfiguration configuration);

      abstract Builder<T> setCompression(Compression compression);

      abstract Builder<T> setCopyStatementPerFile(boolean enabled);

      abstract Write<T> build();
    }
  }

  @AutoValue
  public static abstract class TargetConfiguration implements Serializable {

    @Nullable
    abstract String getJdbcUrl();

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();

    public static TargetConfiguration create() {
      return new AutoValue_RedshiftIO_TargetConfiguration.Builder().build();
    }

    public TargetConfiguration withJdbcUrl(String jdbcUrl) {
      return this.toBuilder()
        .setJdbcUrl(jdbcUrl)
        .build();
    }

    public TargetConfiguration withUsername(String username) {
      return this.toBuilder()
        .setUsername(username)
        .build();
    }

    public TargetConfiguration withPassword(String password) {
      return this.toBuilder()
        .setPassword(password)
        .build();
    }

    DataSource buildDataSource() {
      BasicDataSource ds = new BasicDataSource();
      ds.setDriverClassName("com.amazon.redshift.jdbc.Driver");

      ds.setUsername(getUsername());
      ds.setPassword(getPassword());
      ds.setUrl(getJdbcUrl());

      return ds;
    }

    abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setJdbcUrl(String newJdbcUrl);

      public abstract Builder setUsername(String newUsername);

      public abstract Builder setPassword(String newPassword);

      public abstract TargetConfiguration build();
    }
  }

}
