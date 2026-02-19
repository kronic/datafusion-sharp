namespace DataFusionSharp.Formats.Csv;

/// <summary>
/// Options for writing CSV files.
/// </summary>
public sealed class CsvWriteOptions
{
    /// <summary>
    /// Whether to write a header row. If null, DataFusion uses its default (true).
    /// </summary>
    public bool? HasHeader { get; set; }

    /// <summary>
    /// Column delimiter character. If null, DataFusion uses its default (',').
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Delimiter { get; set; }

    /// <summary>
    /// Quote character. If null, DataFusion uses its default ('"').
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Quote { get; set; }

    /// <summary>
    /// Escape character. If null, DataFusion uses its default (no escape character).
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Escape { get; set; }

    /// <summary>
    /// Compression type for the output CSV file. If null, output is uncompressed.
    /// <para>
    /// <b>Known limitation:</b> Due to the underlying protobuf encoding, <see cref="CompressionType.Gzip"/>
    /// maps to the proto3 zero-value enum entry and is omitted during serialization. Because the Rust
    /// DataFusion layer interprets an omitted compression field as GZIP, requesting
    /// <see cref="CompressionType.Gzip"/> will unexpectedly produce uncompressed output
    /// (the same as <c>null</c>). All other compression types work as expected.
    /// </para>
    /// </summary>
    public CompressionType? Compression { get; set; }

    /// <summary>
    /// Maximum number of records to use for schema inference. If null, DataFusion uses its default.
    /// </summary>
    public ulong? SchemaInferMaxRec { get; set; }

    /// <summary>
    /// Date format string. If null, DataFusion uses its default.
    /// </summary>
    public string? DateFormat { get; set; }

    /// <summary>
    /// Datetime format string. If null, DataFusion uses its default.
    /// </summary>
    public string? DatetimeFormat { get; set; }

    /// <summary>
    /// Timestamp format string. If null, DataFusion uses its default.
    /// </summary>
    public string? TimestampFormat { get; set; }

    /// <summary>
    /// Timestamp with timezone format string. If null, DataFusion uses its default.
    /// </summary>
    public string? TimestampTzFormat { get; set; }

    /// <summary>
    /// Time format string. If null, DataFusion uses its default.
    /// </summary>
    public string? TimeFormat { get; set; }

    /// <summary>
    /// String representation of null values. If null, DataFusion uses its default.
    /// </summary>
    public string? NullValue { get; set; }

    /// <summary>
    /// Regular expression pattern to match null values. If null, DataFusion uses its default.
    /// </summary>
    public string? NullRegex { get; set; }

    /// <summary>
    /// Comment character. If null, DataFusion uses its default.
    /// Must be a single-byte ASCII character.
    /// </summary>
    public char? Comment { get; set; }

    /// <summary>
    /// Whether to double-quote special characters instead of using an escape character.
    /// If null, DataFusion uses its default.
    /// </summary>
    public bool? DoubleQuote { get; set; }

    /// <summary>
    /// Whether newlines in quoted values are supported. If null, DataFusion uses its default.
    /// </summary>
    public bool? NewlinesInValues { get; set; }

    /// <summary>
    /// Line terminator character. If null, DataFusion uses its default.
    /// Must be a single-byte ASCII character (0–127).
    /// </summary>
    public char? Terminator { get; set; }
}
