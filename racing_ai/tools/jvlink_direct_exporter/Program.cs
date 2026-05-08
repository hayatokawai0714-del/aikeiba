using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics;
using System.Threading;

namespace Aikeiba.JVLinkDirectExporter;

internal static class Program
{
    private static readonly string[] RequiredOutputFiles = ["races.csv", "entries.csv", "results.csv", "payouts.csv", "odds.csv"];

    [STAThread]
    public static int Main(string[] args)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var options = CliOptions.Parse(args);
        if (!options.IsValid(out var validationError))
        {
            Console.Error.WriteLine($"[ERROR] {validationError}");
            CliOptions.PrintUsage();
            return 2;
        }

        var logger = new Logger(options.Verbose);
        try
        {
            logger.Info("Aikeiba JV-Link direct exporter started.");
            logger.Info($"race_date={options.RaceDate:yyyy-MM-dd}, output_dir={options.OutputDir}");
            if (options.SetupMode && options.OptionInt is not (3 or 4))
            {
                logger.Warn($"setup-mode expects --option 3 or 4, but got {options.OptionInt}. JVOpen may fail.");
            }

            if (options.ProbeOnly)
            {
                using var jvProbe = JVLinkClient.Create(logger);
                jvProbe.Init(options);
                if (options.ListComMembers)
                {
                    ComIntrospection.PrintDispatchMembers(jvProbe.ComObject, logger);
                    return 0;
                }
                Probe.Run(jvProbe, options, logger);
                return 0;
            }

            if (options.DumpRawOnly)
            {
                PrepareOutputDirectory(options, logger);
                using var jvDump = JVLinkClient.Create(logger);
                jvDump.Init(options);
                RawDump.Run(jvDump, options, logger);
                return 0;
            }

            if (options.DryRun)
            {
                PrepareOutputDirectory(options, logger);
                var dryManifest = RawManifest.Build(
                    options,
                    rowCounts: new Dictionary<string, int>
                    {
                        ["races"] = 0,
                        ["entries"] = 0,
                        ["results"] = 0,
                        ["payouts"] = 0,
                    },
                    warnings:
                    [
                        "dry_run_enabled",
                        "no_jvlink_read_performed",
                    ],
                    missingColumns: new Dictionary<string, List<string>>());
                WriteManifest(options.OutputDir, dryManifest);
                logger.Info("Dry run completed.");
                return 0;
            }

            PrepareOutputDirectory(options, logger);

            using var jv = JVLinkClient.Create(logger);
            jv.Init(options);

            var collector = new RecordCollector(options, logger);

            // In this JV-Link environment, opening RA/SE/HR directly (dataSpec=RA/SE/HR) returns rc=-111.
            // However, weekly specs (RASW/SESW/HRSW) with option=0 are accepted and stream RA/SE/HR records.
            // Default behavior: if user requests dataSpec=RACE, collect from these three streams to build 4 CSVs.
            var normalizedSpec = JVLinkClient.NormalizeDataSpecPublic(options.DataSpec);
            if (options.SetupMode)
            {
                if (!string.Equals(normalizedSpec, "RACE", StringComparison.OrdinalIgnoreCase))
                {
                    logger.Warn($"setup-mode ignores --dataspec={options.DataSpec}; forcing dataspec=RACE per SDK-style setup retrieval.");
                }
                collector.CollectSetupFromRace(jv);
            }
            else if (string.Equals(normalizedSpec, "RACE", StringComparison.OrdinalIgnoreCase))
            {
                if (options.OptionInt == 0)
                {
                    // Backward-compatible path: keep legacy weekly-stream option=0 behavior.
                    var raceSpecs = new[]
                    {
                        new OpenSpec("RASW", 0),
                        new OpenSpec("SESW", 0),
                        new OpenSpec("HRSW", 0),
                        // Wide odds (O3) for EV/selection layer.
                        new OpenSpec("O3SW", 0),
                        // Odds1 (win/place/bracket). Place is useful as market baseline for top3.
                        new OpenSpec("O1SW", 0),
                        // Odds2 (umaren). Pair odds baseline (optional for wide).
                        new OpenSpec("O2SW", 0),
                    };
                    collector.CollectFromSpecs(
                        jv,
                        raceSpecs);
                }
                else
                {
                    // Official-oriented path: open RACE directly and parse RA/SE/HR from JVRead stream.
                    jv.Open(options);
                    collector.AddJVOpenAttempt(new JVOpenAttempt
                    {
                        DataSpec = jv.LastOpenDataSpec,
                        FromTime = jv.LastOpenFromTime,
                        Option = jv.LastOpenOption,
                        Status = jv.IsOpened ? "open_ok" : "open_no_data",
                        ReturnCode = jv.LastOpenReturnCode,
                        ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(jv.LastOpenReturnCode),
                        ReadCount = jv.LastOpenReadCount,
                        DownloadCount = jv.LastOpenDownloadCount,
                    });
                    collector.CollectFrom(jv);
                }
            }
            else
            {
                jv.Open(options);
                collector.CollectFrom(jv);
            }

            var csvWriter = new CsvExporter(options, logger);
            csvWriter.WriteAll(collector);
            if (jv.ReadErrors.Count > 0)
            {
                collector.Warnings.Add($"read_errors_count:{jv.ReadErrors.Count}");
            }
            if (jv.SkippedFiles.Count > 0)
            {
                collector.Warnings.Add($"skipped_files_count:{jv.SkippedFiles.Count}");
            }

            var manifest = RawManifest.Build(
                options,
                collector.RowCounts,
                collector.Warnings,
                csvWriter.MissingColumns,
                collector.JVOpenAttempts,
                options.SetupMode ? collector.SetupReadRecordTypeCounts : collector.LastReadRecordTypeCounts,
                collector.SetupHrRecordCount,
                collector.SetupDateMin,
                collector.SetupDateMax,
                jv.ReadErrors,
                jv.SkippedFiles,
                jv.ReadRetryCount,
                jv.ReadRetrySleepSec);
            WriteManifest(options.OutputDir, manifest);

            logger.Info("Export completed successfully.");
            logger.Info($"rows: races={collector.RowCounts["races"]}, entries={collector.RowCounts["entries"]}, results={collector.RowCounts["results"]}, payouts={collector.RowCounts["payouts"]}, odds={collector.RowCounts["odds"]}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ERROR] {ex.Message}");
            Console.Error.WriteLine(ex.ToString());
            return 1;
        }
    }

    private static void PrepareOutputDirectory(CliOptions options, Logger logger)
    {
        var outputDir = options.OutputDir;
        if (outputDir.Exists)
        {
            if (!options.Overwrite)
            {
                var existing = RequiredOutputFiles.Any(f => File.Exists(Path.Combine(outputDir.FullName, f)));
                if (existing)
                {
                    throw new InvalidOperationException($"Output directory already contains CSVs. Use --overwrite to replace: {outputDir}");
                }
            }
        }
        else
        {
            outputDir.Create();
            logger.Info($"Created output directory: {outputDir}");
        }

        if (options.Overwrite)
        {
            foreach (var fileName in RequiredOutputFiles.Append("raw_manifest_check.json"))
            {
                var target = new FileInfo(Path.Combine(outputDir.FullName, fileName));
                if (target.Exists)
                {
                    target.Delete();
                }
            }
        }
    }

    private static void WriteManifest(DirectoryInfo outputDir, RawManifest manifest)
    {
        var path = new FileInfo(Path.Combine(outputDir.FullName, "raw_manifest_check.json"));
        var json = JsonSerializer.Serialize(manifest, new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        });
        File.WriteAllText(path.FullName, json, new UTF8Encoding(false));
    }
}

internal sealed class CliOptions
{
    public DateOnly RaceDate { get; private init; }
    public DateOnly? EndDate { get; private init; }
    public DirectoryInfo OutputDir { get; private init; } = new DirectoryInfo(".");
    public string OddsSnapshotVersion { get; private init; } = "odds_v1";
    public string CapturedAt { get; private init; } = "";
    public string JvDataDir { get; private init; } = "";
    public bool Overwrite { get; private init; }
    public bool Verbose { get; private init; }
    public bool DryRun { get; private init; }
    public bool ProbeOnly { get; private init; }
    public bool DumpRawOnly { get; private init; }
    public bool ListComMembers { get; private init; }
    public string FromTime { get; private init; } = "";
    public string DataSpec { get; private init; } = "RACE";
    public string Option { get; private init; } = "";
    public string ProbeOptions { get; private init; } = "0,1,2,3,4";
    public int ProbeMaxRecords { get; private init; } = 200;
    public string DumpSpecs { get; private init; } = "RASW:0,SESW:0,HRSW:0";
    public int DumpMaxRecords { get; private init; } = 20000;
    public bool DebugJVOpen { get; private init; }
    public bool SetupMode { get; private init; }
    public int ReadRetryCount { get; private init; } = 2;
    public int ReadRetrySleepSec { get; private init; } = 1;
    public bool SkipReadErrors { get; private init; }

    public int OptionInt => int.TryParse(Option, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : 0;

    public static CliOptions Parse(string[] args)
    {
        DateOnly raceDate = default;
        DateOnly? endDate = null;
        var outputDir = new DirectoryInfo(".");
        var oddsSnapshotVersion = "odds_v1";
        var capturedAt = "";
        var jvDataDir = "";
        var overwrite = false;
        var verbose = false;
        var dryRun = false;
        var probeOnly = false;
        var dumpRawOnly = false;
        var listComMembers = false;
        var fromTime = "";
        var dataSpec = "RACE";
        var option = "";
        var probeOptions = "0,1,2,3,4";
        var probeMaxRecords = 200;
        var dumpSpecs = "RASW:0,SESW:0,HRSW:0";
        var dumpMaxRecords = 20000;
        var debugJvOpen = false;
        var setupMode = false;
        var readRetryCount = 2;
        var readRetrySleepSec = 1;
        var skipReadErrors = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--race-date":
                    raceDate = DateOnly.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--output-dir":
                    outputDir = new DirectoryInfo(args[++i]);
                    break;
                case "--end-date":
                    endDate = DateOnly.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--odds-snapshot-version":
                    oddsSnapshotVersion = args[++i];
                    break;
                case "--captured-at":
                    capturedAt = args[++i];
                    break;
                case "--jv-data-dir":
                    jvDataDir = args[++i];
                    break;
                case "--overwrite":
                    overwrite = true;
                    break;
                case "--verbose":
                    verbose = true;
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--probe-only":
                    probeOnly = true;
                    break;
                case "--dump-raw-only":
                    dumpRawOnly = true;
                    break;
                case "--list-com-members":
                    listComMembers = true;
                    break;
                case "--fromtime":
                    fromTime = args[++i];
                    break;
                case "--dataspec":
                    dataSpec = args[++i];
                    break;
                case "--option":
                    option = args[++i];
                    break;
                case "--probe-options":
                    probeOptions = args[++i];
                    break;
                case "--probe-max-records":
                    probeMaxRecords = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--dump-specs":
                    dumpSpecs = args[++i];
                    break;
                case "--dump-max-records":
                    dumpMaxRecords = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--debug-jvopen":
                    debugJvOpen = true;
                    break;
                case "--setup-mode":
                    setupMode = true;
                    break;
                case "--read-retry-count":
                    readRetryCount = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--read-retry-sleep-sec":
                    readRetrySleepSec = int.Parse(args[++i], CultureInfo.InvariantCulture);
                    break;
                case "--skip-read-errors":
                    skipReadErrors = true;
                    break;
                case "--help":
                case "-h":
                case "/?":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
                default:
                    throw new ArgumentException($"Unknown argument: {arg}");
            }
        }

        if (string.IsNullOrWhiteSpace(option))
        {
            option = setupMode ? "4" : "1";
        }

        if (string.IsNullOrWhiteSpace(fromTime))
        {
            var effectiveEnd = endDate ?? raceDate;
            fromTime = $"{raceDate:yyyyMMdd}000000-{effectiveEnd:yyyyMMdd}235959";
        }

        return new CliOptions
        {
            RaceDate = raceDate,
            EndDate = endDate,
            OutputDir = outputDir,
            OddsSnapshotVersion = oddsSnapshotVersion,
            CapturedAt = capturedAt,
            JvDataDir = jvDataDir,
            Overwrite = overwrite,
            Verbose = verbose,
            DryRun = dryRun,
            ProbeOnly = probeOnly,
            DumpRawOnly = dumpRawOnly,
            ListComMembers = listComMembers,
            FromTime = fromTime,
            DataSpec = dataSpec,
            Option = option,
            ProbeOptions = probeOptions,
            ProbeMaxRecords = probeMaxRecords,
            DumpSpecs = dumpSpecs,
            DumpMaxRecords = dumpMaxRecords,
            DebugJVOpen = debugJvOpen,
            SetupMode = setupMode,
            ReadRetryCount = Math.Max(0, readRetryCount),
            ReadRetrySleepSec = Math.Max(0, readRetrySleepSec),
            SkipReadErrors = skipReadErrors,
        };
    }

    public bool IsValid(out string message)
    {
        if (RaceDate == default)
        {
            message = "--race-date is required (YYYY-MM-DD).";
            return false;
        }

        if (OutputDir.FullName.Trim().Length == 0)
        {
            message = "--output-dir is required.";
            return false;
        }
        if (EndDate.HasValue && EndDate.Value < RaceDate)
        {
            message = "--end-date must be >= --race-date.";
            return false;
        }

        if (!string.IsNullOrWhiteSpace(JvDataDir) && !Directory.Exists(JvDataDir))
        {
            message = $"--jv-data-dir does not exist: {JvDataDir}";
            return false;
        }

        message = "";
        return true;
    }

    public static void PrintUsage()
    {
        Console.WriteLine("Aikeiba JV-Link direct exporter");
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project tools/jvlink_direct_exporter/Aikeiba.JVLinkDirectExporter.csproj -- --race-date 2026-03-30 --output-dir data/raw/20260330_real --overwrite --verbose");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --race-date YYYY-MM-DD   target race date (required)");
        Console.WriteLine("  --end-date YYYY-MM-DD    setup mode end date (optional; default: race-date)");
        Console.WriteLine("  --output-dir PATH        output directory (required)");
        Console.WriteLine("  --odds-snapshot-version  odds_snapshot_version written to odds.csv (default: odds_v1)");
        Console.WriteLine("  --captured-at ISO8601    captured_at written to odds.csv (default: now)");
        Console.WriteLine("  --jv-data-dir PATH       override JV-Link local data dir (example: C:\\ProgramData\\JRA-VAN\\Data Lab)");
        Console.WriteLine("  --overwrite              replace existing output files");
        Console.WriteLine("  --verbose                verbose logging");
        Console.WriteLine("  --fromtime STRING        JVOpen fromTime (default: YYYYMMDD000000-YYYYMMDD235959)");
        Console.WriteLine("  --dataspec STRING        JVOpen data spec (default: RACE)");
        Console.WriteLine("  --option INT             JVOpen option (default: normal=1, setup=4)");
        Console.WriteLine("  --dry-run                no JVLink read; only creates manifest");
        Console.WriteLine("  --probe-only             do not export; probe JVOpen/JVRead behavior");
        Console.WriteLine("  --dump-raw-only          dump raw JV records to text files and exit");
        Console.WriteLine("  --list-com-members       (with --probe-only) list COM IDispatch member names and exit");
        Console.WriteLine("  --probe-options LIST     comma-separated options to probe (default: 0,1,2,3,4)");
        Console.WriteLine("  --probe-max-records N    max raw records to read per probe (default: 200)");
        Console.WriteLine("  --dump-specs LIST        comma-separated specs like RASW:0,SESW:0,HRSW:0");
        Console.WriteLine("  --dump-max-records N     max records per dump stream (default: 20000)");
        Console.WriteLine("  --debug-jvopen           log JVOpen args/return details and write jvopen failures into manifest");
        Console.WriteLine("  --setup-mode             setup retrieval mode (explicit option=3/4 expected for historical backfill)");
        Console.WriteLine("  --read-retry-count N     retry count for JVRead/JVGets transient read errors (default: 2)");
        Console.WriteLine("  --read-retry-sleep-sec N sleep seconds between read retries (default: 1)");
        Console.WriteLine("  --skip-read-errors       skip unresolved read errors instead of throwing");
    }
}

internal sealed class Logger(bool verbose)
{
    private readonly bool _verbose = verbose;

    public void Info(string message) => Console.WriteLine($"[INFO] {message}");

    public void Warn(string message) => Console.WriteLine($"[WARN] {message}");

    public void Debug(string message)
    {
        if (_verbose)
        {
            Console.WriteLine($"[DEBUG] {message}");
        }
    }
}

internal static class Probe
{
    public static void Run(JVLinkClient jv, CliOptions options, Logger logger)
    {
        logger.Info("Probe mode enabled. No CSVs will be exported.");
        DumpComMembers(jv, logger);

        var optionTokens = (options.ProbeOptions ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var optionList = new List<int>();
        foreach (var t in optionTokens)
        {
            if (int.TryParse(t, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            {
                optionList.Add(v);
            }
        }
        if (optionList.Count == 0)
        {
            optionList.Add(1);
        }

        var fromTime = string.IsNullOrWhiteSpace(options.FromTime)
            ? options.RaceDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "000000"
            : options.FromTime;

        // Probe a few likely dataSpecs. "RACE" is known to work in this environment.
        var specs = new List<string> { NormalizeSpec(options.DataSpec) };
        foreach (var s in new[] { "RACE", "RA", "SE", "HR", "RASW", "SESW", "HRSW" })
        {
            var ns = NormalizeSpec(s);
            if (!specs.Contains(ns))
            {
                specs.Add(ns);
            }
        }

        foreach (var spec in specs)
        {
            foreach (var opt in optionList)
            {
                logger.Info($"[PROBE] JVOpen spec={spec} fromTime={fromTime} option={opt}");
                try
                {
                    jv.OpenDirect(spec, fromTime, opt);
                }
                catch (Exception ex)
                {
                    logger.Warn($"[PROBE] JVOpen failed: {ex.GetType().Name}: {ex.Message}");
                    continue;
                }
                if (!jv.IsOpened)
                {
                    logger.Info("[PROBE] JVOpen returned no data; skip JVRead.");
                    continue;
                }

                var typeCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var read = 0;
                try
                {
                    foreach (var rec in jv.ReadRawRecords())
                    {
                        read++;
                        var t = rec.Length >= 2 ? rec.Substring(0, 2) : "??";
                        if (!typeCounts.ContainsKey(t))
                        {
                            typeCounts[t] = 0;
                        }
                        typeCounts[t]++;
                        if (read >= options.ProbeMaxRecords)
                        {
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.Warn($"[PROBE] JVRead failed: {ex.GetType().Name}: {ex.Message}");
                    jv.CloseIfOpened();
                    continue;
                }

                var summary = string.Join(", ", typeCounts.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}:{kv.Value}"));
                logger.Info($"[PROBE] Read raw records={read}, types={summary}");
                jv.CloseIfOpened();
            }
        }

        logger.Info("Probe finished.");
    }

    private static string NormalizeSpec(string spec) => (spec ?? "").Replace(" ", "").ToUpperInvariant();

    private static void DumpComMembers(JVLinkClient jv, Logger logger)
    {
        try
        {
            var t = jv.ComType;
            logger.Info($"[PROBE] COM type={t.FullName}");
            var names = t.GetMembers()
                .Select(m => m.Name)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var filtered = names
                .Where(n =>
                    n.StartsWith("JV", StringComparison.OrdinalIgnoreCase) ||
                    n.Contains("Get", StringComparison.OrdinalIgnoreCase) ||
                    n.Contains("Set", StringComparison.OrdinalIgnoreCase))
                .ToList();

            logger.Info($"[PROBE] COM members(filtered) count={filtered.Count}");
            foreach (var n in filtered.Take(120))
            {
                logger.Info($"[PROBE] member={n}");
            }
        }
        catch (Exception ex)
        {
            logger.Warn($"[PROBE] dump COM members failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

}

internal static class RawDump
{
    public static void Run(JVLinkClient jv, CliOptions options, Logger logger)
    {
        var specs = ParseSpecs(options.DumpSpecs);
        if (specs.Count == 0)
        {
            throw new InvalidOperationException("--dump-specs is empty. Example: RASW:0,SESW:0,HRSW:0");
        }

        var fromTime = string.IsNullOrWhiteSpace(options.FromTime)
            ? options.RaceDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "000000"
            : options.FromTime;

        var root = options.OutputDir.FullName;
        Directory.CreateDirectory(root);

        var summary = new List<object>();
        foreach (var spec in specs)
        {
            var fileName = $"raw_dump_{spec.DataSpec}_opt{spec.Option}.txt";
            var path = Path.Combine(root, fileName);
            logger.Info($"[DUMP] opening dataSpec={spec.DataSpec}, option={spec.Option}, fromTime={fromTime}");

            jv.OpenDirect(spec.DataSpec, fromTime, spec.Option);
            if (!jv.IsOpened)
            {
                logger.Info($"[DUMP] no data for dataSpec={spec.DataSpec}, option={spec.Option}, fromTime={fromTime}");
                summary.Add(new
                {
                    data_spec = spec.DataSpec,
                    option = spec.Option,
                    from_time = fromTime,
                    output_path = path,
                    record_count = 0,
                    record_types = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase),
                    no_data = true,
                });
                continue;
            }

            var count = 0;
            var typeCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
            foreach (var rawRecord in jv.ReadRawRecords())
            {
                count++;
                writer.WriteLine(rawRecord);
                var type = rawRecord.Length >= 2 ? rawRecord[..2].ToUpperInvariant() : "??";
                if (!typeCounts.TryAdd(type, 1))
                {
                    typeCounts[type]++;
                }
                if (count >= options.DumpMaxRecords)
                {
                    logger.Warn($"[DUMP] reached --dump-max-records={options.DumpMaxRecords} for {spec.DataSpec}");
                    break;
                }
            }

            jv.CloseIfOpened();
            logger.Info($"[DUMP] wrote {path} (records={count})");
            summary.Add(new
            {
                data_spec = spec.DataSpec,
                option = spec.Option,
                from_time = fromTime,
                output_path = path,
                record_count = count,
                record_types = typeCounts.OrderBy(kv => kv.Key).ToDictionary(kv => kv.Key, kv => kv.Value),
            });
        }

        var summaryPath = Path.Combine(root, "raw_dump_summary.json");
        var json = JsonSerializer.Serialize(
            new
            {
                race_date = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                created_at = DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture),
                from_time = fromTime,
                dumps = summary,
            },
            new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(summaryPath, json, new UTF8Encoding(false));
        logger.Info($"[DUMP] wrote {summaryPath}");
    }

    private static List<OpenSpec> ParseSpecs(string? value)
    {
        var items = (value ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var result = new List<OpenSpec>();
        foreach (var item in items)
        {
            var parts = item.Split(':', 2, StringSplitOptions.TrimEntries);
            var dataSpec = parts[0].ToUpperInvariant();
            var option = 0;
            if (parts.Length == 2 && !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out option))
            {
                option = 0;
            }
            result.Add(new OpenSpec(dataSpec, option));
        }
        return result;
    }
}

internal sealed class JVLinkClient : IDisposable
{
    private readonly Logger _logger;
    private readonly dynamic _com;
    private bool _opened;
    private bool _debugJVOpen;
    private int? _lastOpenReturnCode;
    private int? _lastOpenReadCount;
    private int? _lastOpenDownloadCount;
    private string _lastOpenDataSpec = "";
    private string _lastOpenFromTime = "";
    private int _lastOpenOption;
    private int _readRetryCount = 2;
    private int _readRetrySleepSec = 1;
    private bool _skipReadErrors;
    private readonly List<ReadErrorEntry> _readErrors = [];
    private readonly List<string> _skippedFiles = [];

    public bool IsOpened => _opened;
    public int? LastOpenReturnCode => _lastOpenReturnCode;
    public int? LastOpenReadCount => _lastOpenReadCount;
    public int? LastOpenDownloadCount => _lastOpenDownloadCount;
    public string LastOpenDataSpec => _lastOpenDataSpec;
    public string LastOpenFromTime => _lastOpenFromTime;
    public int LastOpenOption => _lastOpenOption;
    public IReadOnlyList<ReadErrorEntry> ReadErrors => _readErrors;
    public IReadOnlyList<string> SkippedFiles => _skippedFiles;
    public int ReadRetryCount => _readRetryCount;
    public int ReadRetrySleepSec => _readRetrySleepSec;
    public bool SkipReadErrors => _skipReadErrors;

    private JVLinkClient(dynamic com, Logger logger)
    {
        _com = com;
        _logger = logger;
    }

    public Type ComType => ((object)_com).GetType();
    public object ComObject => (object)_com;

    public static JVLinkClient Create(Logger logger)
    {
        string[] progIds = ["JVDTLab.JVLink", "JVLink.JVLink"];
        Exception? last = null;

        foreach (var progId in progIds)
        {
            try
            {
                var type = Type.GetTypeFromProgID(progId, throwOnError: false);
                if (type is null)
                {
                    continue;
                }

                var instance = Activator.CreateInstance(type);
                if (instance is null)
                {
                    continue;
                }

                logger.Info($"Connected COM ProgID: {progId}");
                return new JVLinkClient(instance, logger);
            }
            catch (Exception ex)
            {
                last = ex;
            }
        }

        throw new InvalidOperationException(
            "JV-Link COM object was not found. Install JV-Link SDK and complete JV-DataLab authentication.",
            last);
    }

    public void Init(CliOptions options)
    {
        _debugJVOpen = options.DebugJVOpen;
        _readRetryCount = options.ReadRetryCount;
        _readRetrySleepSec = options.ReadRetrySleepSec;
        _skipReadErrors = options.SkipReadErrors;
        _readErrors.Clear();
        _skippedFiles.Clear();
        _logger.Debug("Calling JVInit...");
        dynamic jv = _com;
        var appId = "AikeibaJVDirectExporter";
        if (_debugJVOpen)
        {
            _logger.Info($"[JVINIT] appId={appId}");
        }
        try
        {
            var rc = Convert.ToInt32(jv.JVInit(appId));
            if (rc != 0)
            {
                throw new InvalidOperationException($"JVInit failed: rc={rc}");
            }
        }
        catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException)
        {
            var rc = Convert.ToInt32(jv.JVInit());
            if (rc != 0)
            {
                throw new InvalidOperationException($"JVInit failed: rc={rc}");
            }
        }

        _logger.Info("JVInit succeeded.");

        if (!string.IsNullOrWhiteSpace(options.JvDataDir))
        {
            TrySetJvDataDir(options.JvDataDir);
        }
    }

    private void TrySetJvDataDir(string dir)
    {
        // JV-Link exposes different setter names depending on version/environment.
        // We'll attempt a small set of common method names. If none exists, we warn and continue.
        dynamic jv = _com;
        var candidates = new[]
        {
            "JVSetSavePath",
            "JVSetDataPath",
            "JVSetDownloadPath",
            "SetSavePath",
            "SetDataPath",
            "SetDownloadPath",
        };

        foreach (var name in candidates)
        {
            try
            {
                var result = ((object)jv).GetType().InvokeMember(
                    name,
                    System.Reflection.BindingFlags.InvokeMethod,
                    null,
                    jv,
                    new object[] { dir });
                _logger.Info($"JV-Link local data dir set via {name}: {dir}");
                return;
            }
            catch (System.Reflection.TargetInvocationException ex)
            {
                _logger.Debug($"JV data dir setter {name} failed: {ex.InnerException?.GetType().Name ?? ex.GetType().Name}");
            }
            catch (MissingMethodException)
            {
            }
            catch (Exception ex)
            {
                _logger.Debug($"JV data dir setter {name} failed: {ex.GetType().Name}");
            }
        }

        _logger.Warn($"--jv-data-dir was provided but no setter method was found on JV-Link COM. dir={dir}");
    }

    public void Open(CliOptions options)
    {
        _logger.Debug("Calling JVOpen...");
        dynamic jv = _com;

        var dataSpec = NormalizeDataSpec(options.DataSpec);
        var fromTime = options.FromTime;
        var option = int.TryParse(options.Option, out var op) ? op : 1;
        OpenDirectInternal(jv, dataSpec, fromTime, option);
    }

    public void OpenDirect(string dataSpec, string fromTime, int option)
    {
        _logger.Debug("Calling JVOpen (direct)...");
        dynamic jv = _com;
        var spec = NormalizeDataSpec(dataSpec);
        OpenDirectInternal(jv, spec, fromTime, option);
    }

    private void OpenDirectInternal(dynamic jv, string dataSpec, string fromTime, int option)
    {
        _lastOpenReturnCode = null;
        _lastOpenReadCount = null;
        _lastOpenDownloadCount = null;
        _lastOpenDataSpec = dataSpec;
        _lastOpenFromTime = fromTime;
        _lastOpenOption = option;
        if (_debugJVOpen)
        {
            _logger.Info($"[JVOPEN] request dataSpec={dataSpec}, fromTime={fromTime}, option={option}");
        }
        int rc = TryJVOpenExactSignature(jv, dataSpec, fromTime, option)
            ?? TryJVOpenKnownSignatures(jv, dataSpec, fromTime, option)
            ?? InvokeJVOpenFlexible(jv, dataSpec, fromTime, option);
        _lastOpenReturnCode = rc;
        if (_debugJVOpen)
        {
            _logger.Info($"[JVOPEN] result rc={rc}, dataSpec={dataSpec}, fromTime={fromTime}, option={option}");
        }

        if (rc < 0)
        {
            // Some JV-Link environments return rc=-1 for dates/specs with no available data yet.
            // Treat it as "no data" so range exports can proceed without aborting the whole run.
            if (rc == -1)
            {
                _opened = false;
                _logger.Warn($"JVOpen returned rc=-1 (no data). dataSpec={dataSpec}, fromTime={fromTime}, option={option}");
                return;
            }
            throw new JVOpenFailedException(dataSpec, fromTime, option, rc);
        }

        _opened = true;
        _logger.Info($"JVOpen succeeded. dataSpec={dataSpec}, fromTime={fromTime}, option={option}");
    }

    public void CloseIfOpened()
    {
        if (!_opened)
        {
            return;
        }
        try
        {
            dynamic jv = _com;
            jv.JVClose();
        }
        catch
        {
        }
        _opened = false;
    }

    private int? TryJVOpenExactSignature(dynamic jv, string dataSpec, string fromTime, int option)
    {
        try
        {
            var args = new object[] { dataSpec, fromTime, option, 0, 0, string.Empty };
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try exact args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, readCount=0, downloadCount=0, lastFile='')");
            }
            var result = ((object)jv).GetType().InvokeMember(
                "JVOpen",
                System.Reflection.BindingFlags.InvokeMethod,
                null,
                jv,
                args);
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] exact returned rc={result}");
            }
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen exact signature failed: {ex.GetType().Name}");
            return null;
        }
    }

    private static string NormalizeDataSpec(string rawDataSpec)
    {
        var spec = (rawDataSpec ?? "").Trim();
        if (spec.Length == 0)
        {
            return "RACE";
        }

        // Keep user intent. Some environments accept composite specs like "RA,SE,HR".
        // We only normalize whitespace/casing.
        return spec.Replace(" ", "").ToUpperInvariant();
    }

    public static string NormalizeDataSpecPublic(string rawDataSpec) => NormalizeDataSpec(rawDataSpec);

    private int? TryJVOpenKnownSignatures(dynamic jv, string dataSpec, string fromTime, int option)
    {
        try
        {
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig3 args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option})");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(dataSpec,fromTime,option) failed: {ex.GetType().Name}");
        }

        try
        {
            // Some SDKs expect option as string ("1") instead of int.
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig3(option_str) args=(dataSpec={dataSpec}, fromTime={fromTime}, option='{option.ToString(CultureInfo.InvariantCulture)}')");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option.ToString(CultureInfo.InvariantCulture)));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(dataSpec,fromTime,option_str) failed: {ex.GetType().Name}");
        }

        try
        {
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig2 args=(dataSpec={dataSpec}, fromTime={fromTime})");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(dataSpec,fromTime) failed: {ex.GetType().Name}");
        }

        try
        {
            int readCount = 0;
            int downloadCount = 0;
            var rc = Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, ref readCount, ref downloadCount));
            _lastOpenReadCount = readCount;
            _lastOpenDownloadCount = downloadCount;
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig5(ref int,int) args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, readCount={readCount}, downloadCount={downloadCount}) rc={rc}");
            }
            return rc;
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,ref int,ref int) failed: {ex.GetType().Name}");
        }

        try
        {
            long readCount = 0;
            long downloadCount = 0;
            var rc = Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, ref readCount, ref downloadCount));
            _lastOpenReadCount = readCount > int.MaxValue ? int.MaxValue : (int)readCount;
            _lastOpenDownloadCount = downloadCount > int.MaxValue ? int.MaxValue : (int)downloadCount;
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig5(ref long,long) args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, readCount={readCount}, downloadCount={downloadCount}) rc={rc}");
            }
            return rc;
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,ref long,ref long) failed: {ex.GetType().Name}");
        }

        try
        {
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig4(int) args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, 0)");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, 0));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,int) failed: {ex.GetType().Name}");
        }

        try
        {
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig4(string) args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, '')");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, ""));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,string) failed: {ex.GetType().Name}");
        }

        try
        {
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig6 args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, 0, 0, '')");
            }
            return Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, 0, 0, ""));
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,int,int,string) failed: {ex.GetType().Name}");
        }

        try
        {
            int readCount = 0;
            int downloadCount = 0;
            string lastFile = "";
            var rc = Convert.ToInt32(jv.JVOpen(dataSpec, fromTime, option, ref readCount, ref downloadCount, ref lastFile));
            _lastOpenReadCount = readCount;
            _lastOpenDownloadCount = downloadCount;
            if (_debugJVOpen)
            {
                _logger.Info($"[JVOPEN] try sig6(ref int,int,string) args=(dataSpec={dataSpec}, fromTime={fromTime}, option={option}, readCount={readCount}, downloadCount={downloadCount}, lastFile='{lastFile}') rc={rc}");
            }
            return rc;
        }
        catch (Exception ex)
        {
            _logger.Debug($"JVOpen(...,ref int,ref int,ref string) failed: {ex.GetType().Name}");
        }

        return null;
    }

    private int InvokeJVOpenFlexible(dynamic jv, string dataSpec, string fromTime, int option)
    {
        Exception? lastError = null;
        var methods = ((object)jv).GetType().GetMethods().Where(m => string.Equals(m.Name, "JVOpen", StringComparison.OrdinalIgnoreCase));
        var candidates = methods.OrderBy(m => m.GetParameters().Length).ToList();
        _logger.Debug($"JVOpen reflection candidates: {candidates.Count}");
        foreach (var method in candidates)
        {
            var parameterText = string.Join(", ", method.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"));
            _logger.Debug($"JVOpen candidate: ({parameterText})");
            var args = BuildJVOpenArgs(method.GetParameters(), dataSpec, fromTime, option);
            if (args is null)
            {
                continue;
            }

            try
            {
                _logger.Debug($"Trying JVOpen signature: {method}");
                var result = method.Invoke(jv, args);
                return Convert.ToInt32(result);
            }
            catch (Exception ex)
            {
                lastError = ex;
            }
        }

        throw new InvalidOperationException("Error while invoking JVOpen.", lastError);
    }

    private static object[]? BuildJVOpenArgs(System.Reflection.ParameterInfo[] parameters, string dataSpec, string fromTime, int option)
    {
        var args = new object[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            var parameterType = p.ParameterType.IsByRef ? p.ParameterType.GetElementType()! : p.ParameterType;

            if (i == 0 && parameterType == typeof(string))
            {
                args[i] = dataSpec;
                continue;
            }

            if (i == 1 && parameterType == typeof(string))
            {
                args[i] = fromTime;
                continue;
            }

            if (i == 2)
            {
                try
                {
                    args[i] = Convert.ChangeType(option, parameterType, CultureInfo.InvariantCulture);
                    continue;
                }
                catch
                {
                    if (parameterType == typeof(string))
                    {
                        args[i] = option.ToString(CultureInfo.InvariantCulture);
                        continue;
                    }
                }
            }

            if (parameterType == typeof(string))
            {
                args[i] = string.Empty;
                continue;
            }

            if (parameterType == typeof(bool))
            {
                args[i] = false;
                continue;
            }

            if (parameterType.IsPrimitive || parameterType == typeof(decimal))
            {
                args[i] = Activator.CreateInstance(parameterType)!;
                continue;
            }

            return null;
        }

        return args;
    }

    public IEnumerable<string> ReadRawRecords()
    {
        dynamic jv = _com;
        while (true)
        {
            string record;
            string fileName;
            int rc;
            var sw = Stopwatch.StartNew();
            if (!TryReadRecord(jv, out record, out rc, out fileName))
            {
                throw new InvalidOperationException("JVRead/JVGets invocation failed. Check SDK version/signature in Program.cs.");
            }
            sw.Stop();
            _logger.Debug($"[JVREAD] rc={rc} file_name={fileName} record_count={(string.IsNullOrWhiteSpace(record) ? 0 : 1)} elapsed_sec={sw.Elapsed.TotalSeconds:F3}");

            if (rc == 0)
            {
                yield break;
            }

            if (rc < 0)
            {
                if (rc == -1)
                {
                    if (string.IsNullOrWhiteSpace(record))
                    {
                        _logger.Warn("JVRead/JVGets returned rc=-1 with empty buffer. No readable data was returned (target date not downloaded yet or JV-Link data state not ready).");
                        yield break;
                    }
                }
                else if (rc == -3 || rc == -203)
                {
                    var resolved = false;
                    for (var retry = 1; retry <= _readRetryCount; retry++)
                    {
                        TryJVStatus(jv);
                        if (_readRetrySleepSec > 0)
                        {
                            Thread.Sleep(TimeSpan.FromSeconds(_readRetrySleepSec));
                        }

                        if (!TryReadRecord(jv, out record, out rc, out fileName))
                        {
                            continue;
                        }
                        if (rc >= 0 || (rc == -1 && !string.IsNullOrWhiteSpace(record)))
                        {
                            resolved = true;
                            break;
                        }
                    }

                    if (!resolved)
                    {
                        var meaning = rc == -203 ? "read_error_unknown_203" : "read_error_buffer_or_temp_unavailable";
                        _readErrors.Add(new ReadErrorEntry
                        {
                            ReturnCode = rc,
                            ReturnCodeMeaning = meaning,
                            FileName = fileName,
                            RecordCount = string.IsNullOrWhiteSpace(record) ? 0 : 1,
                            ElapsedSec = sw.Elapsed.TotalSeconds,
                        });

                        if (_skipReadErrors)
                        {
                            var skipped = string.IsNullOrWhiteSpace(fileName) ? "(unknown_file)" : fileName.Trim();
                            _skippedFiles.Add(skipped);
                            _logger.Warn($"Skipping read error rc={rc} file={skipped}");
                            continue;
                        }

                        throw new InvalidOperationException($"JVRead/JVGets returned error: rc={rc}");
                    }
                }
                else
                {
                    throw new InvalidOperationException($"JVRead/JVGets returned error: rc={rc}");
                }
            }

            if (!string.IsNullOrWhiteSpace(record))
            {
                yield return record;
            }
        }
    }

    private static bool TryReadRecord(dynamic jv, out string record, out int rc, out string fileName)
    {
        record = string.Empty;
        rc = -999999;
        fileName = string.Empty;

        const int initialBufferSize = 256 * 1024;
        return TryReadWithBuffers(jv, initialBufferSize, out record, out rc, out fileName);
    }

    private static bool TryReadWithBuffers(dynamic jv, int bufferSize, out string record, out int rc, out string fileName)
    {
        record = string.Empty;
        rc = -999999;
        fileName = string.Empty;

        int[] bufferCandidates =
        [
            Math.Max(bufferSize, 256 * 1024),
            2 * 1024 * 1024,
            8 * 1024 * 1024,
            16 * 1024 * 1024,
        ];

        foreach (var candidate in bufferCandidates.Distinct())
        {
            if (TryJVRead(jv, candidate, out record, out rc, out fileName))
            {
                if (rc == -3)
                {
                    continue;
                }
                return true;
            }

            if (TryJVGets(jv, candidate, out record, out rc, out fileName))
            {
                if (rc == -3)
                {
                    continue;
                }
                return true;
            }
        }

        rc = -3;
        return false;
    }

    private static bool TryJVRead(dynamic jv, int bufferSize, out string record, out int rc, out string fileName)
    {
        record = string.Empty;
        rc = -999999;
        fileName = string.Empty;

        try
        {
            string line = new string(' ', bufferSize);
            string file = new string(' ', 512);
            rc = Convert.ToInt32(jv.JVRead(ref line, bufferSize, ref file), CultureInfo.InvariantCulture);
            record = line;
            fileName = file;
            return true;
        }
        catch { }

        try
        {
            var args = new object[] { new string(' ', bufferSize), bufferSize, new string(' ', 512) };
            var result = ((object)jv).GetType().InvokeMember("JVRead", System.Reflection.BindingFlags.InvokeMethod, null, jv, args);
            rc = Convert.ToInt32(result, CultureInfo.InvariantCulture);
            record = args[0]?.ToString() ?? string.Empty;
            fileName = args.Length >= 3 ? (args[2]?.ToString() ?? string.Empty) : string.Empty;
            return true;
        }
        catch { }

        try
        {
            string line = new string(' ', bufferSize);
            string file = new string(' ', 512);
            rc = Convert.ToInt32(jv.JVRead(line, bufferSize, file), CultureInfo.InvariantCulture);
            record = line;
            fileName = file;
            return true;
        }
        catch { }

        try
        {
            string line = new string(' ', bufferSize);
            rc = Convert.ToInt32(jv.JVRead(ref line));
            record = line;
            return true;
        }
        catch { }

        return false;
    }

    private static bool TryJVGets(dynamic jv, int bufferSize, out string record, out int rc, out string fileName)
    {
        record = string.Empty;
        rc = -999999;
        fileName = string.Empty;

        try
        {
            var args = new object[] { (object)new string(' ', bufferSize), bufferSize, new string(' ', 512) };
            var result = ((object)jv).GetType().InvokeMember("JVGets", System.Reflection.BindingFlags.InvokeMethod, null, jv, args);
            rc = Convert.ToInt32(result, CultureInfo.InvariantCulture);
            record = args[0]?.ToString() ?? string.Empty;
            fileName = args.Length >= 3 ? (args[2]?.ToString() ?? string.Empty) : string.Empty;
            return true;
        }
        catch { }

        try
        {
            object line = new string(' ', bufferSize);
            string file = new string(' ', 512);
            rc = Convert.ToInt32(jv.JVGets(line, bufferSize, file), CultureInfo.InvariantCulture);
            record = line?.ToString() ?? string.Empty;
            fileName = file;
            return true;
        }
        catch { }

        try
        {
            string line = new string(' ', bufferSize);
            rc = Convert.ToInt32(jv.JVGets(ref line));
            record = line;
            return true;
        }
        catch { }

        return false;
    }

    private void TryJVStatus(dynamic jv)
    {
        try
        {
            var statusObj = jv.JVStatus();
            _logger.Debug($"[JVSTATUS] {statusObj}");
        }
        catch
        {
            try
            {
                object status = 0;
                ((object)jv).GetType().InvokeMember(
                    "JVStatus",
                    System.Reflection.BindingFlags.InvokeMethod,
                    null,
                    jv,
                    new object[] { status });
            }
            catch
            {
            }
        }
    }

    public void Dispose()
    {
        try
        {
            if (_opened)
            {
                dynamic jv = _com;
                try
                {
                    jv.JVClose();
                }
                catch
                {
                }
            }
        }
        finally
        {
            _opened = false;
        }
    }
}

internal sealed class JVOpenFailedException(string dataSpec, string fromTime, int option, int rc)
    : InvalidOperationException($"JVOpen failed: rc={rc}, dataSpec={dataSpec}, fromTime={fromTime}, option={option}")
{
    public string DataSpec { get; } = dataSpec;
    public string FromTime { get; } = fromTime;
    public int Option { get; } = option;
    public int ReturnCode { get; } = rc;
}

internal static class JVOpenReturnCode
{
    public static string ToMeaning(int? rc)
    {
        if (rc is null) return "unknown";
        return rc.Value switch
        {
            0 => "success",
            -1 => "no_data",
            -111 => "invalid_dataspec",
            _ => "unknown_rc",
        };
    }
}

internal sealed class RecordCollector
{
    private readonly CliOptions _options;
    private readonly Logger _logger;
    private readonly List<RaceRow> _races = [];
    private readonly List<EntryRow> _entries = [];
    private readonly List<ResultRow> _results = [];
    private readonly List<PayoutRow> _payouts = [];
    private readonly List<OddsRow> _odds = [];
    private readonly Dictionary<string, int> _skippedRaceDateCounts = new(StringComparer.Ordinal);
    private bool _seenO3 = false;
    private bool _seenO1 = false;
    private bool _seenO2 = false;
    private readonly Dictionary<string, int> _setupReadRecordTypeCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _lastReadRecordTypeCounts = new(StringComparer.OrdinalIgnoreCase);
    private string? _setupDateMin;
    private string? _setupDateMax;
    private int _setupHrRecordCount = 0;

    public Dictionary<string, int> RowCounts => new()
    {
        ["races"] = _races.Count,
        ["entries"] = _entries.Count,
        ["results"] = _results.Count,
        ["payouts"] = _payouts.Count,
        ["odds"] = _odds.Count,
    };

    public List<string> Warnings { get; } = [];
    public List<JVOpenAttempt> JVOpenAttempts { get; } = [];

    public IReadOnlyList<RaceRow> Races => _races;
    public IReadOnlyList<EntryRow> Entries => _entries;
    public IReadOnlyList<ResultRow> Results => _results;
    public IReadOnlyList<PayoutRow> Payouts => _payouts;
    public IReadOnlyList<OddsRow> Odds => _odds;
    public IReadOnlyDictionary<string, int> SetupReadRecordTypeCounts => _setupReadRecordTypeCounts;
    public IReadOnlyDictionary<string, int> LastReadRecordTypeCounts => _lastReadRecordTypeCounts;
    public int SetupHrRecordCount => _setupHrRecordCount;
    public string? SetupDateMin => _setupDateMin;
    public string? SetupDateMax => _setupDateMax;

    public RecordCollector(CliOptions options, Logger logger)
    {
        _options = options;
        _logger = logger;
    }

    public void AddJVOpenAttempt(JVOpenAttempt attempt)
    {
        JVOpenAttempts.Add(attempt);
    }

    public void CollectSetupFromRace(JVLinkClient jv)
    {
        _races.Clear();
        _entries.Clear();
        _results.Clear();
        _payouts.Clear();
        _odds.Clear();
        _setupReadRecordTypeCounts.Clear();
        _setupDateMin = null;
        _setupDateMax = null;
        _setupHrRecordCount = 0;

        var fromTime = string.IsNullOrWhiteSpace(_options.FromTime)
            ? _options.RaceDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "000000"
            : _options.FromTime;
        var option = _options.OptionInt;
        if (option is not (3 or 4))
        {
            Warnings.Add($"setup_option_not_recommended:{option}");
        }

        var attempt = new JVOpenAttempt
        {
            DataSpec = "RACE",
            FromTime = fromTime,
            Option = option,
            Status = "started",
            ReturnCode = null,
            ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(null),
        };

        try
        {
            jv.OpenDirect("RACE", fromTime, option);
        }
        catch (JVOpenFailedException ex)
        {
            attempt.Status = "open_failed";
            attempt.ReturnCode = ex.ReturnCode;
            attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(ex.ReturnCode);
            attempt.ReadCount = jv.LastOpenReadCount;
            attempt.DownloadCount = jv.LastOpenDownloadCount;
            JVOpenAttempts.Add(attempt);
            Warnings.Add($"jvopen_failed:{ex.DataSpec}:{ex.FromTime}:{ex.Option}:rc={ex.ReturnCode}");
            _logger.Warn(ex.Message);
            return;
        }

        if (!jv.IsOpened)
        {
            attempt.Status = "open_no_data";
            attempt.ReturnCode = -1;
            attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(-1);
            attempt.ReadCount = jv.LastOpenReadCount;
            attempt.DownloadCount = jv.LastOpenDownloadCount;
            JVOpenAttempts.Add(attempt);
            Warnings.Add("jvopen_no_data:RACE");
            return;
        }

        var startDate = _options.RaceDate;
        var endDate = _options.EndDate ?? _options.RaceDate;
        var rawReadCount = 0;
        foreach (var rawRecord in jv.ReadRawRecords())
        {
            rawReadCount++;
            if (rawRecord.Length < 2)
            {
                continue;
            }
            var recType = rawRecord.Substring(0, 2);
            if (!_setupReadRecordTypeCounts.TryAdd(recType, 1))
            {
                _setupReadRecordTypeCounts[recType]++;
            }

            var recDate8 = TryExtractRaceDate8FromRaw(rawRecord, recType);
            if (!string.IsNullOrWhiteSpace(recDate8))
            {
                if (_setupDateMin is null || string.CompareOrdinal(recDate8, _setupDateMin) < 0) _setupDateMin = recDate8;
                if (_setupDateMax is null || string.CompareOrdinal(recDate8, _setupDateMax) > 0) _setupDateMax = recDate8;
                if (DateOnly.TryParseExact(recDate8, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var recDate) && recDate > endDate)
                {
                    Warnings.Add("setup_read_stopped_by_end_date");
                    break;
                }
            }

            if (!string.Equals(recType, "HR", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            _setupHrRecordCount++;

            var parsed = ParsedRecord.FromRaw(rawRecord);
            var raceDate = NormalizeDate8(parsed.GetAny("race_date", "kaisai_date")) ?? recDate8;
            if (string.IsNullOrWhiteSpace(raceDate))
            {
                continue;
            }
            if (!DateOnly.TryParseExact(raceDate, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var hrDate))
            {
                continue;
            }
            if (hrDate < startDate || hrDate > endDate)
            {
                continue;
            }

            _payouts.AddRange(BuildPayouts(parsed, raceDate));
        }

        jv.CloseIfOpened();
        attempt.Status = "read_ok";
        attempt.ReturnCode = 0;
        attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(0);
        attempt.ReadCount = rawReadCount;
        attempt.DownloadCount = jv.LastOpenDownloadCount;
        JVOpenAttempts.Add(attempt);
        _logger.Info($"Setup read complete: total_raw={rawReadCount}, hr_records={_payouts.Count}");

        _races.Clear();
        _entries.Clear();
        _results.Clear();
        _odds.Clear();
        _payouts.RemoveAll(r => string.IsNullOrWhiteSpace(r.RaceId));
        if (_payouts.Count == 0)
        {
            Warnings.Add("no_hr_records_parsed");
        }
    }

    public void CollectFromSpecs(JVLinkClient jv, IEnumerable<OpenSpec> specs)
    {
        _odds.Clear();
        _seenO3 = false;
        _seenO1 = false;
        _seenO2 = false;
        var totalRead = 0;
        var totalTypes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        var baseFromTime = string.IsNullOrWhiteSpace(_options.FromTime)
            // setup mode uses explicit race-date anchor; normal mode uses +1 day creation-date anchor.
            ? (_options.SetupMode
                ? _options.RaceDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "000000"
                : _options.RaceDate.AddDays(1).ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "000000")
            : _options.FromTime;

        // Some weekly streams (RASW/SESW/...) may not return the exact day when opened with fromTime=target day.
        // Empirically:
        // - Many records are keyed by "data creation date" (often race_date + 1 day).
        // - Some race days (e.g., holidays / abnormal schedules) can appear only when opened with a later creation date.
        // Therefore we try a small window around baseFromTime, both backwards and forwards, and stop as soon as we parse
        // any RA races for the target date.
        var lookbackDays = string.IsNullOrWhiteSpace(_options.FromTime) ? (_options.SetupMode ? 0 : 6) : 0;
        var lookaheadDays = string.IsNullOrWhiteSpace(_options.FromTime) ? (_options.SetupMode ? 0 : 6) : 0;
        var fromTimesToTry = new List<string> { baseFromTime };
        if ((lookbackDays > 0 || lookaheadDays > 0) && TryParseDateTime14(baseFromTime, out var baseDt))
        {
            for (var i = 1; i <= lookbackDays; i++)
            {
                var dt = baseDt.AddDays(-i);
                fromTimesToTry.Add(dt.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture));
            }

            for (var i = 1; i <= lookaheadDays; i++)
            {
                var dt = baseDt.AddDays(i);
                fromTimesToTry.Add(dt.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture));
            }
        }

        // Production path:
        // - normal mode: option=0 (stable path)
        // - setup mode: option must be explicit 3/4 by caller (historical setup retrieval)
        var optionCandidates = _options.SetupMode
            ? new List<int> { _options.OptionInt }
            : new List<int> { 0 };
        if (_options.DebugJVOpen)
        {
            foreach (var o in _options.SetupMode
                ? new[] { 3, 4, _options.OptionInt, 0, 1, 2 }
                : new[] { _options.OptionInt, 1, 2, 3, 4 })
            {
                if (!optionCandidates.Contains(o))
                {
                    optionCandidates.Add(o);
                }
            }
        }

        foreach (var fromTime in fromTimesToTry)
        {
            foreach (var opt in optionCandidates)
            {
                // On each attempt, clear parsed outputs so we don't mix multiple tries.
                if (_races.Count > 0 || _entries.Count > 0 || _results.Count > 0 || _payouts.Count > 0 || _odds.Count > 0)
                {
                    _races.Clear();
                    _entries.Clear();
                    _results.Clear();
                    _payouts.Clear();
                    _odds.Clear();
                    _seenO3 = false;
                    _seenO1 = false;
                    _seenO2 = false;
                }

                foreach (var s0 in specs)
                {
                    var s = s0 with { Option = opt };
                    var attempt = new JVOpenAttempt
                    {
                        DataSpec = s.DataSpec,
                        FromTime = fromTime,
                        Option = s.Option,
                        Status = "started",
                        ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(null),
                    };
                    _logger.Info($"Collecting from JVLink stream: dataSpec={s.DataSpec}, option={s.Option}");
                    try
                    {
                        jv.OpenDirect(s.DataSpec, fromTime, s.Option);
                    }
                    catch (JVOpenFailedException ex)
                    {
                        attempt.Status = "open_failed";
                        attempt.ReturnCode = ex.ReturnCode;
                        attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(ex.ReturnCode);
                        attempt.ReadCount = jv.LastOpenReadCount;
                        attempt.DownloadCount = jv.LastOpenDownloadCount;
                        JVOpenAttempts.Add(attempt);
                        Warnings.Add($"jvopen_failed:{ex.DataSpec}:{ex.FromTime}:{ex.Option}:rc={ex.ReturnCode}");
                        _logger.Warn(ex.Message);
                        continue;
                    }
                    catch (InvalidOperationException ex)
                    {
                        attempt.Status = "open_failed";
                        attempt.ReturnCode = -99999;
                        attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(-99999);
                        JVOpenAttempts.Add(attempt);
                        Warnings.Add($"jvopen_failed:{s.DataSpec}:{fromTime}:{s.Option}");
                        _logger.Warn(ex.Message);
                        continue;
                    }
                    if (!jv.IsOpened)
                    {
                        attempt.Status = "open_no_data";
                        attempt.ReturnCode = -1;
                        attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(-1);
                        attempt.ReadCount = jv.LastOpenReadCount;
                        attempt.DownloadCount = jv.LastOpenDownloadCount;
                        JVOpenAttempts.Add(attempt);
                        Warnings.Add($"jvopen_no_data:{s.DataSpec}:{fromTime}:{s.Option}");
                        continue;
                    }
                    attempt.Status = "open_ok";
                    attempt.ReturnCode = 0;
                    attempt.ReturnCodeMeaning = JVOpenReturnCode.ToMeaning(0);
                    attempt.DownloadCount = jv.LastOpenDownloadCount;
                    int read;
                    Dictionary<string, int> types;
                    try
                    {
                        (read, types) = CollectFromCore(jv, previewLimit: 1);
                        attempt.ReadCount = read;
                        attempt.Status = "read_ok";
                        JVOpenAttempts.Add(attempt);
                    }
                    catch (InvalidOperationException ex)
                    {
                        if (ex.Message.Contains("JVRead/JVGets invocation failed", StringComparison.Ordinal))
                        {
                            attempt.Status = "read_failed";
                            JVOpenAttempts.Add(attempt);
                            Warnings.Add($"jvread_failed:{s.DataSpec}:{fromTime}:{s.Option}");
                            _logger.Warn($"JV read failed and will be treated as no-data. dataSpec={s.DataSpec}, fromTime={fromTime}, option={s.Option}");
                            jv.CloseIfOpened();
                            continue;
                        }

                        throw;
                    }
                    totalRead += read;
                    foreach (var kv in types)
                    {
                        if (!totalTypes.TryAdd(kv.Key, kv.Value))
                        {
                            totalTypes[kv.Key] += kv.Value;
                        }
                    }
                    jv.CloseIfOpened();
                }

                if (_races.Count > 0)
                {
                    if (!string.Equals(fromTime, baseFromTime, StringComparison.Ordinal))
                    {
                        // Note: since we search both directions, just record the actual fromTime used.
                        Warnings.Add($"fromtime_override_used:{fromTime}");
                    }
                    if (opt != _options.OptionInt)
                    {
                        Warnings.Add($"option_fallback_used:{opt}");
                    }
                    goto Done;
                }
            }
        }

Done:
        FinalizeCollection(totalRead, totalTypes);
    }

    public void CollectFrom(JVLinkClient jv)
    {
        _odds.Clear();
        _seenO3 = false;
        _seenO1 = false;
        _seenO2 = false;
        var (rawReadCount, rawTypeCount) = CollectFromCore(jv, previewLimit: 3);
        FinalizeCollection(rawReadCount, rawTypeCount);
    }

    private (int rawReadCount, Dictionary<string, int> rawTypeCount) CollectFromCore(JVLinkClient jv, int previewLimit)
    {
        var raceDateText = _options.RaceDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var rawReadCount = 0;
        var rawTypeCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawRecord in jv.ReadRawRecords())
        {
            rawReadCount++;
            if (rawReadCount <= previewLimit)
            {
                var preview = rawRecord.Replace("\r", "\\r").Replace("\n", "\\n");
                if (preview.Length > 160)
                {
                    preview = preview[..160] + "...";
                }
                _logger.Debug($"raw_record_preview[{rawReadCount}]={preview}");
            }
            var parsed = ParsedRecord.FromRaw(rawRecord);
            if (parsed.Type.Length < 2)
            {
                continue;
            }
            if (!rawTypeCount.TryAdd(parsed.Type, 1))
            {
                rawTypeCount[parsed.Type]++;
            }

            var extractedRaceDate = TryExtractRaceDate8FromRaw(rawRecord, parsed.Type);
            var raceDateSource = parsed.GetAny("race_date", "kaisai_date");
            if (string.IsNullOrWhiteSpace(raceDateSource))
            {
                raceDateSource = extractedRaceDate;
            }
            if (string.IsNullOrWhiteSpace(raceDateSource))
            {
                raceDateSource = raceDateText;
            }

            // For RA/SE/HR/O1/O2/O3 fixed-width records, the yyyy+mmdd fields already represent
            // the actual race date. "nichiji/kai_day" is the meeting day index and must not be
            // added as a calendar offset; doing so drops valid weekend races like 2025-08-02.
            var raceDate = NormalizeDate8(raceDateSource) ?? raceDateText;

            if (!string.Equals(raceDate, raceDateText, StringComparison.Ordinal))
            {
                if (!string.IsNullOrWhiteSpace(raceDate))
                {
                    if (!_skippedRaceDateCounts.TryAdd(raceDate, 1))
                    {
                        _skippedRaceDateCounts[raceDate]++;
                    }
                }
                continue;
            }

            switch (parsed.Type)
            {
                case "RA":
                    _races.Add(BuildRace(parsed, raceDate));
                    break;
                case "SE":
                    _entries.Add(BuildEntry(parsed, raceDate));
                    _results.Add(BuildResult(parsed, raceDate));
                    break;
                case "HR":
                    _payouts.AddRange(BuildPayouts(parsed, raceDate));
                    break;
                case "O3":
                    _seenO3 = true;
                    _odds.AddRange(BuildWideOdds(parsed, raceDate, _options));
                    break;
                case "O1":
                    _seenO1 = true;
                    _odds.AddRange(BuildO1Odds(parsed, raceDate, _options));
                    break;
                case "O2":
                    _seenO2 = true;
                    _odds.AddRange(BuildO2Odds(parsed, raceDate, _options));
                    break;
            }
        }

        return (rawReadCount, rawTypeCount);
    }

    private static string? TryExtractRaceDate8FromRaw(string rawRecord, string recordType)
    {
        // Expected examples:
        //   RA7<created_yyyymmdd><race_yyyymmdd>...
        //   SE7<created_yyyymmdd><race_yyyymmdd>...
        //   HR2<created_yyyymmdd><race_yyyymmdd>...
        //   O1?<created_yyyymmdd><race_yyyymmdd>...
        // We use 0-based indices: [0..1]=type, [2]=data_kbn, [3..10]=created date, [11..18]=race date.
        if (recordType is not ("RA" or "SE" or "HR" or "O3" or "O1" or "O2"))
        {
            return null;
        }

        if (string.IsNullOrEmpty(rawRecord) || rawRecord.Length < 19)
        {
            return null;
        }

        try
        {
            var date = rawRecord.Substring(11, 8);
            return NormalizeDate8(date);
        }
        catch
        {
            return null;
        }
    }

    private static bool TryParseDateTime14(string value, out DateTime dt)
    {
        return DateTime.TryParseExact(value, "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.None, out dt);
    }

    private void FinalizeCollection(int rawReadCount, Dictionary<string, int> rawTypeCount)
    {
        _lastReadRecordTypeCounts.Clear();
        foreach (var kv in rawTypeCount)
        {
            _lastReadRecordTypeCounts[kv.Key] = kv.Value;
        }

        _races.RemoveAll(r => string.IsNullOrWhiteSpace(r.RaceId));
        _entries.RemoveAll(r => string.IsNullOrWhiteSpace(r.RaceId) || !r.Umaban.HasValue || r.Umaban.Value <= 0);
        _results.RemoveAll(r => string.IsNullOrWhiteSpace(r.RaceId) || !r.Umaban.HasValue || r.Umaban.Value <= 0 || !r.FinishPos.HasValue);
        _payouts.RemoveAll(r => string.IsNullOrWhiteSpace(r.RaceId));

        if (_races.Count == 0) Warnings.Add("no_ra_records_parsed");
        if (_entries.Count == 0) Warnings.Add("no_se_records_for_entries_parsed");
        if (_results.Count == 0) Warnings.Add("no_se_records_for_results_parsed");
        if (_payouts.Count == 0) Warnings.Add("no_hr_records_parsed");

        if (rawReadCount == 0)
        {
            Warnings.Add("no_raw_records_read_from_jvlink");
        }
        else
        {
            var hasTargetTypes = rawTypeCount.ContainsKey("RA") || rawTypeCount.ContainsKey("SE") || rawTypeCount.ContainsKey("HR");
            if (!hasTargetTypes && (rawTypeCount.ContainsKey("H1") || rawTypeCount.ContainsKey("JG")))
            {
                Warnings.Add("jvlink_stream_missing_ra_se_hr_records");
                Warnings.Add("hint: JVOpen did not return RA/SE/HR. Try probe mode: --probe-only --dataspec RACE --probe-options 0,1,2,3,4");
            }
        }
        if (_races.Count == 0 && _entries.Count == 0 && _skippedRaceDateCounts.Count > 0)
        {
            var availableDates = _skippedRaceDateCounts
                .OrderByDescending(kv => kv.Value)
                .ThenByDescending(kv => kv.Key)
                .Take(5)
                .Select(kv => $"{kv.Key}:{kv.Value}")
                .ToList();
            var availableDateText = string.Join(",", availableDates);
            Warnings.Add("target_date_records_missing");
            Warnings.Add($"available_record_dates={availableDateText}");
            _logger.Warn($"No records matched target race_date={_options.RaceDate:yyyyMMdd}. Available record dates: {availableDateText}");
        }
        _logger.Info($"Raw records read={rawReadCount}");
        if (rawTypeCount.Count > 0)
        {
            var typeSummary = string.Join(", ", rawTypeCount.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}:{kv.Value}"));
            _logger.Info($"Raw record types={typeSummary}");
        }

        // Build odds.csv (Aikeiba raw odds schema) from SE parsed win odds (単勝オッズ).
        // Wide odds are Phase 2; for now we export win odds only (odds_type=win).
        var cap = string.IsNullOrWhiteSpace(_options.CapturedAt)
            ? DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture)
            : _options.CapturedAt.Trim();

        // Keep O3 (wide odds) parsed during CollectFromCore; then append win odds from SE.
        foreach (var e in _entries)
        {
            if (string.IsNullOrWhiteSpace(e.RaceId))
            {
                continue;
            }
            if (!e.Umaban.HasValue || e.Umaban.Value <= 0)
            {
                continue;
            }

            _odds.Add(new OddsRow
            {
                RaceId = e.RaceId,
                OddsSnapshotVersion = _options.OddsSnapshotVersion,
                CapturedAt = cap,
                OddsType = "win",
                HorseNo = e.Umaban.Value,
                HorseNoA = -1,
                HorseNoB = -1,
                OddsValue = e.Odds,
                SourceVersion = _options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            });
        }

        if (_seenO3 && !_odds.Any(o => string.Equals(o.OddsType, "wide", StringComparison.OrdinalIgnoreCase)))
        {
            Warnings.Add("o3_seen_but_no_wide_odds_parsed");
        }
        if (_seenO1 && !_odds.Any(o => string.Equals(o.OddsType, "place", StringComparison.OrdinalIgnoreCase)))
        {
            Warnings.Add("o1_seen_but_no_place_odds_parsed");
        }
        if (_seenO2 && !_odds.Any(o => string.Equals(o.OddsType, "umaren", StringComparison.OrdinalIgnoreCase)))
        {
            Warnings.Add("o2_seen_but_no_umaren_odds_parsed");
        }
        if (!_odds.Any(o => string.Equals(o.OddsType, "win", StringComparison.OrdinalIgnoreCase)))
        {
            Warnings.Add("no_win_odds_parsed_from_se");
        }

        _logger.Info($"Parsed records: races={_races.Count}, entries={_entries.Count}, results={_results.Count}, payouts={_payouts.Count}, odds={_odds.Count}");
    }

    private static IEnumerable<OddsRow> BuildWideOdds(ParsedRecord p, string raceDate, CliOptions options)
    {
        // O3 record ("オッズ３（ワイド）") - fixed width:
        // - wide odds blocks start at position 41, block length 17 bytes, total 153 blocks.
        // - block:
        //   (1) kumi (4) e.g. 0102 => 01-02
        //   (5) min odds (5) => odds*10, 99999 => 9999.9, 00000 => null
        //   (10) max odds (5) same rule
        //   (15) popularity (3)
        //
        // We export:
        //   - odds_type=wide with odds_value=min_odds (conservative)
        //   - odds_type=wide_max with odds_value=max_odds
        var cap = string.IsNullOrWhiteSpace(options.CapturedAt)
            ? DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture)
            : options.CapturedAt.Trim();

        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        var outRows = new List<OddsRow>(153 * 2);
        var raw = p.Values.TryGetValue("raw_record", out var rr) ? rr : "";
        if (string.IsNullOrWhiteSpace(raw))
        {
            return outRows;
        }
        var bytes = Encoding.GetEncoding(932).GetBytes(raw);

        static string SliceBytes(byte[] b, int start1, int length)
        {
            if (length <= 0 || start1 <= 0) return string.Empty;
            var start = start1 - 1;
            if (start >= b.Length) return string.Empty;
            var len = Math.Min(length, b.Length - start);
            var seg = Encoding.GetEncoding(932).GetString(b, start, len);
            return seg.TrimEnd('\0').Trim();
        }

        for (var i = 0; i < 153; i++)
        {
            var basePos = 41 + i * 17;
            var kumi = SliceBytes(bytes, basePos, 4);
            if (string.IsNullOrWhiteSpace(kumi))
            {
                continue;
            }

            var digits = new string(kumi.Where(char.IsDigit).ToArray());
            if (digits.Length != 4)
            {
                continue;
            }
            var a = int.Parse(digits[..2], CultureInfo.InvariantCulture);
            var b = int.Parse(digits.Substring(2, 2), CultureInfo.InvariantCulture);
            if (a <= 0 || b <= 0 || a == b)
            {
                continue;
            }

            var minOdds = ParseOdds10(SliceBytes(bytes, basePos + 4, 5));
            var maxOdds = ParseOdds10(SliceBytes(bytes, basePos + 9, 5));

            if (minOdds.HasValue)
            {
                outRows.Add(new OddsRow
                {
                    RaceId = raceId,
                    OddsSnapshotVersion = options.OddsSnapshotVersion,
                    CapturedAt = cap,
                    OddsType = "wide",
                    HorseNo = -1,
                    HorseNoA = a,
                    HorseNoB = b,
                    OddsValue = minOdds.Value,
                    SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                });
            }

            if (maxOdds.HasValue)
            {
                outRows.Add(new OddsRow
                {
                    RaceId = raceId,
                    OddsSnapshotVersion = options.OddsSnapshotVersion,
                    CapturedAt = cap,
                    OddsType = "wide_max",
                    HorseNo = -1,
                    HorseNoA = a,
                    HorseNoB = b,
                    OddsValue = maxOdds.Value,
                    SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                });
            }
        }
        return outRows;
    }

    private static IEnumerable<OddsRow> BuildO1Odds(ParsedRecord p, string raceDate, CliOptions options)
    {
        // O1 record ("オッズ１（単複枠）") - fixed width:
        // - win odds blocks start at position 44, block length 8 bytes, total 28 blocks.
        //   (1) horse_no (2)
        //   (3) odds (4) => odds*10, "9999" => 999.9, "0000" => null
        //   (7) popularity (2)
        // - place odds blocks start at position 268, block length 12 bytes, total 28 blocks.
        //   (1) horse_no (2)
        //   (3) min odds (4) => odds*10
        //   (7) max odds (4) => odds*10
        //   (11) popularity (2)
        // - bracket odds blocks start at position 604, block length 9 bytes, total 36 blocks.
        //   (1) combo (2) => 1-1 .. 8-8 (encoded)
        //   (3) odds (5) => odds*10
        //   (8) popularity (2)
        //
        // We export:
        //   - odds_type=place with odds_value=min_place
        //   - odds_type=place_max with odds_value=max_place
        //   - odds_type=bracket with odds_value=bracket_odds (horse_no_a/b = waku_a/b)
        var cap = string.IsNullOrWhiteSpace(options.CapturedAt)
            ? DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture)
            : options.CapturedAt.Trim();

        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        var raw = p.Values.TryGetValue("raw_record", out var rr) ? rr : "";
        if (string.IsNullOrWhiteSpace(raw))
        {
            return [];
        }
        var bytes = Encoding.GetEncoding(932).GetBytes(raw);

        static string SliceBytes(byte[] b, int start1, int length)
        {
            if (length <= 0 || start1 <= 0) return string.Empty;
            var start = start1 - 1;
            if (start >= b.Length) return string.Empty;
            var len = Math.Min(length, b.Length - start);
            var seg = Encoding.GetEncoding(932).GetString(b, start, len);
            return seg.TrimEnd('\0').Trim();
        }

        var outRows = new List<OddsRow>(28 * 2 + 36);

        // place
        for (var i = 0; i < 28; i++)
        {
            var basePos = 268 + i * 12;
            var hText = SliceBytes(bytes, basePos, 2);
            if (!int.TryParse(new string(hText.Where(char.IsDigit).ToArray()), NumberStyles.Integer, CultureInfo.InvariantCulture, out var h) || h <= 0)
            {
                continue;
            }

            var min = ParseOdds10(SliceBytes(bytes, basePos + 2, 4));
            var max = ParseOdds10(SliceBytes(bytes, basePos + 6, 4));

            if (min.HasValue)
            {
                outRows.Add(new OddsRow
                {
                    RaceId = raceId,
                    OddsSnapshotVersion = options.OddsSnapshotVersion,
                    CapturedAt = cap,
                    OddsType = "place",
                    HorseNo = h,
                    HorseNoA = -1,
                    HorseNoB = -1,
                    OddsValue = min.Value,
                    SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                });
            }

            if (max.HasValue)
            {
                outRows.Add(new OddsRow
                {
                    RaceId = raceId,
                    OddsSnapshotVersion = options.OddsSnapshotVersion,
                    CapturedAt = cap,
                    OddsType = "place_max",
                    HorseNo = h,
                    HorseNoA = -1,
                    HorseNoB = -1,
                    OddsValue = max.Value,
                    SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                });
            }
        }

        // bracket
        for (var i = 0; i < 36; i++)
        {
            var basePos = 604 + i * 9;
            var comboRaw = SliceBytes(bytes, basePos, 2);
            var digits = new string(comboRaw.Where(char.IsDigit).ToArray());
            if (digits.Length != 2)
            {
                continue;
            }
            var a = int.Parse(digits[..1], CultureInfo.InvariantCulture);
            var b = int.Parse(digits.Substring(1, 1), CultureInfo.InvariantCulture);
            if (a <= 0 || b <= 0)
            {
                continue;
            }

            var odds = ParseOdds10(SliceBytes(bytes, basePos + 2, 5));
            if (!odds.HasValue)
            {
                continue;
            }

            outRows.Add(new OddsRow
            {
                RaceId = raceId,
                OddsSnapshotVersion = options.OddsSnapshotVersion,
                CapturedAt = cap,
                OddsType = "bracket",
                HorseNo = -1,
                HorseNoA = a,
                HorseNoB = b,
                OddsValue = odds.Value,
                SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            });
        }

        return outRows;
    }

    private static IEnumerable<OddsRow> BuildO2Odds(ParsedRecord p, string raceDate, CliOptions options)
    {
        // O2 record ("オッズ２（馬連）") - fixed width:
        // - odds blocks start at position 41, block length 13 bytes, total 153 blocks.
        //   (1) kumi (4) e.g. 0102 => 01-02
        //   (5) odds (6) => odds*10, "999999" => max, "000000" => null
        //   (11) popularity (3) (currently unused in Aikeiba raw schema)
        //
        // We export:
        //   - odds_type=umaren with odds_value=odds (pair)
        var cap = string.IsNullOrWhiteSpace(options.CapturedAt)
            ? DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture)
            : options.CapturedAt.Trim();

        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        var raw = p.Values.TryGetValue("raw_record", out var rr) ? rr : "";
        if (string.IsNullOrWhiteSpace(raw))
        {
            return [];
        }
        var bytes = Encoding.GetEncoding(932).GetBytes(raw);

        static string SliceBytes(byte[] b, int start1, int length)
        {
            if (length <= 0 || start1 <= 0) return string.Empty;
            var start = start1 - 1;
            if (start >= b.Length) return string.Empty;
            var len = Math.Min(length, b.Length - start);
            var seg = Encoding.GetEncoding(932).GetString(b, start, len);
            return seg.TrimEnd('\0').Trim();
        }

        var outRows = new List<OddsRow>(153);
        for (var i = 0; i < 153; i++)
        {
            var basePos = 41 + i * 13;
            var kumi = SliceBytes(bytes, basePos, 4);
            if (string.IsNullOrWhiteSpace(kumi))
            {
                continue;
            }

            var digits = new string(kumi.Where(char.IsDigit).ToArray());
            if (digits.Length != 4)
            {
                continue;
            }
            var a = int.Parse(digits[..2], CultureInfo.InvariantCulture);
            var b = int.Parse(digits.Substring(2, 2), CultureInfo.InvariantCulture);
            if (a <= 0 || b <= 0 || a == b)
            {
                continue;
            }

            var odds = ParseOdds10(SliceBytes(bytes, basePos + 4, 6));
            if (!odds.HasValue)
            {
                continue;
            }

            outRows.Add(new OddsRow
            {
                RaceId = raceId,
                OddsSnapshotVersion = options.OddsSnapshotVersion,
                CapturedAt = cap,
                OddsType = "umaren",
                HorseNo = -1,
                HorseNoA = a,
                HorseNoB = b,
                OddsValue = odds.Value,
                SourceVersion = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            });
        }

        return outRows;
    }

    private static RaceRow BuildRace(ParsedRecord p, string raceDate)
    {
        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        return new RaceRow
        {
            RaceId = raceId,
            RaceDate = ToDashDate(raceDate),
            VenueCode = venue,
            Venue = p.GetAny("venue", "jyo_name") ?? venue,
            RaceNo = raceNo,
            RaceName = p.GetAny("race_name", "name") ?? "",
            Distance = ParseInt(p.GetAny("distance", "kyori")),
            Surface = p.GetAny("surface", "track_type", "shiba_dirt") ?? "",
            TrackCondition = p.GetAny("track_condition", "baba") ?? "",
            FieldSize = ParseInt(p.GetAny("field_size", "head_count")),
            Grade = p.GetAny("grade", "class") ?? "",
        };
    }

    private static EntryRow BuildEntry(ParsedRecord p, string raceDate)
    {
        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        return new EntryRow
        {
            RaceId = raceId,
            HorseId = p.GetAny("horse_id", "ketto_num") ?? "",
            HorseName = p.GetAny("horse_name", "bamei") ?? "",
            Umaban = ParseInt(p.GetAny("umaban", "horse_no")),
            Waku = ParseInt(p.GetAny("waku", "wakuban")),
            JockeyName = p.GetAny("jockey_name", "kishu_name") ?? "",
            TrainerName = p.GetAny("trainer_name", "chokyoshi_name") ?? "",
            WeightCarried = ParseDecimal(p.GetAny("weight_carried", "futan")),
            Odds = ParseDecimal(p.GetAny("odds", "tansho_odds")),
            Popularity = ParseInt(p.GetAny("popularity", "ninki")),
        };
    }

    private static ResultRow BuildResult(ParsedRecord p, string raceDate)
    {
        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        return new ResultRow
        {
            RaceId = raceId,
            HorseId = p.GetAny("horse_id", "ketto_num") ?? "",
            Umaban = ParseInt(p.GetAny("umaban", "horse_no")),
            FinishPos = ParseInt(p.GetAny("finish_pos", "chakujun")),
            Time = p.GetAny("time", "race_time") ?? "",
            Margin = ParseDecimal(p.GetAny("margin", "chakusa")),
            Corner1Pos = ParseInt(p.GetAny("corner1_pos", "corner1")),
            Corner2Pos = ParseInt(p.GetAny("corner2_pos", "corner2")),
            Corner3Pos = ParseInt(p.GetAny("corner3_pos", "corner3")),
            Corner4Pos = ParseInt(p.GetAny("corner4_pos", "corner4")),
            Last3F = ParseDecimal(p.GetAny("last3f", "agari3f")),
            Odds = ParseDecimal(p.GetAny("odds", "tansho_odds")),
            Popularity = ParseInt(p.GetAny("popularity", "ninki")),
        };
    }

    private static IEnumerable<PayoutRow> BuildPayouts(ParsedRecord p, string raceDate)
    {
        var venue = p.GetAny("venue_code", "jyo_cd", "jyo", "venue") ?? "";
        var raceNo = ParseInt(p.GetAny("race_no", "race_num", "r")).GetValueOrDefault();
        var raceId = BuildRaceId(raceDate, venue, raceNo);

        var one = new PayoutRow
        {
            RaceId = raceId,
            BetType = p.GetAny("bet_type", "syubetu") ?? "",
            WinningCombination = p.GetAny("winning_combination", "kumi", "umaban") ?? "",
            PayoutYen = ParseInt(p.GetAny("payout_yen", "haraimodoshi", "payout")),
        };

        if (!string.IsNullOrWhiteSpace(one.BetType) || !string.IsNullOrWhiteSpace(one.WinningCombination) || one.PayoutYen.HasValue)
        {
            yield return one;
            yield break;
        }

        foreach (var kv in p.Values)
        {
            if (!kv.Key.Contains("_", StringComparison.Ordinal))
            {
                continue;
            }

            var parts = kv.Key.Split('_', 2);
            var betType = parts[0].ToUpperInvariant();
            var combo = parts.Length > 1 ? parts[1] : "";
            if (!int.TryParse(kv.Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var payoutYen))
            {
                continue;
            }

            yield return new PayoutRow
            {
                RaceId = raceId,
                BetType = betType,
                WinningCombination = combo,
                PayoutYen = payoutYen,
            };
        }
    }

    private static string BuildRaceId(string raceDate8, string venueRaw, int raceNo)
    {
        if (raceNo <= 0)
        {
            return "";
        }

        var venue = NormalizeVenue(venueRaw);
        if (string.IsNullOrWhiteSpace(venue))
        {
            return "";
        }

        return $"{raceDate8}-{venue}-{raceNo:00}R";
    }

    private static string NormalizeVenue(string value)
    {
        var token = (value ?? "").Trim().ToUpperInvariant();
        return token switch
        {
            "01" => "SAP",
            "02" => "HAK",
            "03" => "FUK",
            "04" => "NII",
            "05" => "TOK",
            "06" => "NAK",
            "07" => "CHU",
            "08" => "KYO",
            "09" => "HAN",
            "10" => "KOK",
            "SAP" or "HAK" or "FUK" or "NII" or "TOK" or "NAK" or "CHU" or "KYO" or "HAN" or "KOK" => token,
            _ => token.Length >= 3 ? token[..3] : token,
        };
    }

    private static int? ParseInt(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return int.TryParse(value.Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var x) ? x : null;
    }

    private static decimal? ParseDecimal(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return decimal.TryParse(value.Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var x) ? x : null;
    }

    private static decimal? ParseOdds10(string? value)
    {
        // Odds in JV O3 blocks are stored as 5-digit integer where 1 decimal place is implied:
        //  - "02401" => 240.1
        //  - "00000" => not available
        //  - "99999" => 9999.9 or more (clamped to 9999.9)
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var digits = new string(value.Trim().Where(char.IsDigit).ToArray());
        if (digits.Length == 0)
        {
            return null;
        }

        if (!int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var x))
        {
            return null;
        }

        if (x <= 0)
        {
            return null;
        }

        if (x >= 99999)
        {
            return 9999.9m;
        }

        return x / 10m;
    }

    private static string? NormalizeDate8(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        var t = value.Replace("-", "").Replace("/", "").Trim();
        return t.Length == 8 && t.All(char.IsDigit) ? t : null;
    }

    private static bool TryParseDate8(string yyyymmdd, out DateOnly date)
    {
        return DateOnly.TryParseExact(yyyymmdd, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
    }

    private static string ToDashDate(string yyyymmdd)
    {
        if (yyyymmdd.Length != 8) return yyyymmdd;
        return $"{yyyymmdd[..4]}-{yyyymmdd.Substring(4, 2)}-{yyyymmdd.Substring(6, 2)}";
    }
}

internal readonly record struct OpenSpec(string DataSpec, int Option);

internal sealed class CsvExporter
{
    private readonly CliOptions _options;
    private readonly Logger _logger;

    public Dictionary<string, List<string>> MissingColumns { get; } = new();

    public CsvExporter(CliOptions options, Logger logger)
    {
        _options = options;
        _logger = logger;
    }

    public void WriteAll(RecordCollector collector)
    {
        WriteRaces(collector.Races);
        WriteEntries(collector.Entries);
        WriteResults(collector.Results);
        WritePayouts(collector.Payouts);
        WriteOdds(collector.Odds);
    }

    private void WriteRaces(IReadOnlyList<RaceRow> rows)
    {
        string[] headers = ["race_id", "race_date", "venue_code", "venue", "race_no", "race_name", "distance", "surface", "track_condition", "field_size", "grade"];
        var path = new FileInfo(Path.Combine(_options.OutputDir.FullName, "races.csv"));
        using var writer = CreateWriter(path);
        WriteRow(writer, headers);
        foreach (var row in rows)
        {
            WriteRow(writer,
            [
                row.RaceId,
                row.RaceDate,
                row.VenueCode,
                row.Venue,
                ToStr(row.RaceNo),
                row.RaceName,
                ToStr(row.Distance),
                row.Surface,
                row.TrackCondition,
                ToStr(row.FieldSize),
                row.Grade,
            ]);
        }
        _logger.Info($"wrote {path.FullName}");
        MissingColumns["races.csv"] = [];
    }

    private void WriteEntries(IReadOnlyList<EntryRow> rows)
    {
        // popularity is frequently missing in SE fixed-width parsing; we derive it from odds (ascending) as fallback.
        var derivedPopularity = new Dictionary<(string raceId, int umaban), int>();
        foreach (var g in rows.Where(r => r.Odds.HasValue && r.Umaban.HasValue).GroupBy(r => r.RaceId))
        {
            var ranked = g.OrderBy(r => r.Odds!.Value).ThenBy(r => r.Umaban!.Value).ToList();
            for (var i = 0; i < ranked.Count; i++)
            {
                derivedPopularity[(g.Key, ranked[i].Umaban!.Value)] = i + 1;
            }
        }

        string[] headers = ["race_id", "horse_id", "horse_name", "umaban", "waku", "jockey_name", "trainer_name", "weight_carried", "odds", "popularity"];
        var path = new FileInfo(Path.Combine(_options.OutputDir.FullName, "entries.csv"));
        using var writer = CreateWriter(path);
        WriteRow(writer, headers);
        foreach (var row in rows)
        {
            var pop = row.Popularity;
            if (!pop.HasValue && row.Umaban.HasValue && derivedPopularity.TryGetValue((row.RaceId, row.Umaban.Value), out var dpop))
            {
                pop = dpop;
            }
            WriteRow(writer,
            [
                row.RaceId,
                row.HorseId,
                row.HorseName,
                ToStr(row.Umaban),
                ToStr(row.Waku),
                row.JockeyName,
                row.TrainerName,
                ToStr(row.WeightCarried),
                ToStr(row.Odds),
                ToStr(pop),
            ]);
        }
        _logger.Info($"wrote {path.FullName}");
        MissingColumns["entries.csv"] = [];
    }

    private void WriteResults(IReadOnlyList<ResultRow> rows)
    {
        string[] headers = ["race_id", "horse_id", "umaban", "finish_pos", "time", "margin", "corner1_pos", "corner2_pos", "corner3_pos", "corner4_pos", "last3f", "odds", "popularity"];
        var path = new FileInfo(Path.Combine(_options.OutputDir.FullName, "results.csv"));
        using var writer = CreateWriter(path);
        WriteRow(writer, headers);
        foreach (var row in rows)
        {
            WriteRow(writer,
            [
                row.RaceId,
                row.HorseId,
                ToStr(row.Umaban),
                ToStr(row.FinishPos),
                row.Time,
                ToStr(row.Margin),
                ToStr(row.Corner1Pos),
                ToStr(row.Corner2Pos),
                ToStr(row.Corner3Pos),
                ToStr(row.Corner4Pos),
                ToStr(row.Last3F),
                ToStr(row.Odds),
                ToStr(row.Popularity),
            ]);
        }
        _logger.Info($"wrote {path.FullName}");
        MissingColumns["results.csv"] = [];
    }

    private void WritePayouts(IReadOnlyList<PayoutRow> rows)
    {
        string[] headers = ["race_id", "bet_type", "winning_combination", "payout_yen"];
        var path = new FileInfo(Path.Combine(_options.OutputDir.FullName, "payouts.csv"));
        using var writer = CreateWriter(path);
        WriteRow(writer, headers);
        foreach (var row in rows)
        {
            WriteRow(writer,
            [
                row.RaceId,
                row.BetType,
                row.WinningCombination,
                ToStr(row.PayoutYen),
            ]);
        }
        _logger.Info($"wrote {path.FullName}");
        MissingColumns["payouts.csv"] = [];
    }

    private void WriteOdds(IReadOnlyList<OddsRow> rows)
    {
        string[] headers =
        [
            "race_id",
            "odds_snapshot_version",
            "captured_at",
            "odds_type",
            "horse_no",
            "horse_no_a",
            "horse_no_b",
            "odds_value",
            "source_version",
        ];

        var path = new FileInfo(Path.Combine(_options.OutputDir.FullName, "odds.csv"));
        using var writer = CreateWriter(path);
        WriteRow(writer, headers);
        foreach (var row in rows)
        {
            WriteRow(writer,
            [
                row.RaceId,
                row.OddsSnapshotVersion,
                row.CapturedAt,
                row.OddsType,
                ToStr(row.HorseNo),
                ToStr(row.HorseNoA),
                ToStr(row.HorseNoB),
                ToStr(row.OddsValue),
                row.SourceVersion,
            ]);
        }
        _logger.Info($"wrote {path.FullName}");
        MissingColumns["odds.csv"] = [];
    }

    private static StreamWriter CreateWriter(FileInfo path)
    {
        path.Directory?.Create();
        var stream = new FileStream(path.FullName, FileMode.Create, FileAccess.Write, FileShare.Read);
        return new StreamWriter(stream, new UTF8Encoding(false));
    }

    private static void WriteRow(StreamWriter writer, IEnumerable<string?> values)
    {
        var escaped = values.Select(EscapeCsv);
        writer.WriteLine(string.Join(",", escaped));
    }

    private static string EscapeCsv(string? value)
    {
        var text = value ?? "";
        if (text.Contains('"')) text = text.Replace("\"", "\"\"");
        if (text.Contains(',') || text.Contains('\n') || text.Contains('\r') || text.Contains('"'))
        {
            return $"\"{text}\"";
        }
        return text;
    }

    private static string ToStr(object? value)
    {
        if (value is null)
        {
            return "";
        }

        if (value is IFormattable formattable)
        {
            return formattable.ToString(null, CultureInfo.InvariantCulture) ?? "";
        }

        return value.ToString() ?? "";
    }
}

internal sealed class ParsedRecord
{
    public string Type { get; private init; } = "";
    public Dictionary<string, string> Values { get; private init; } = new(StringComparer.OrdinalIgnoreCase);

    public static ParsedRecord FromRaw(string raw)
    {
        var text = raw ?? "";
        if (text.Length == 0)
        {
            return new ParsedRecord();
        }

        var type = text.Length >= 2 ? text[..2].ToUpperInvariant() : "";
        var values = ParseLooseKeyValue(text);
        if ((type is "RA" or "SE" or "HR" or "O3" or "O1" or "O2") && FixedWidthJVParser.IsLikelyFixedWidth(text))
        {
            values = FixedWidthJVParser.Parse(type, text);
        }
        values["raw_record"] = text;

        return new ParsedRecord
        {
            Type = type,
            Values = values,
        };
    }

    public bool TryGet(string key, out string value)
    {
        if (Values.TryGetValue(key, out var v))
        {
            value = v;
            return true;
        }

        value = "";
        return false;
    }

    public string? GetAny(params string[] keys)
    {
        foreach (var key in keys)
        {
            if (Values.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v))
            {
                return v;
            }
        }
        return null;
    }

    private static Dictionary<string, string> ParseLooseKeyValue(string text)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var delimiters = new[] { ',', '\t', '|', '^' };
        var tokenized = delimiters.Select(d => text.Split(d)).FirstOrDefault(parts => parts.Count(p => p.Contains('=')) >= 2);
        if (tokenized is not null)
        {
            foreach (var token in tokenized)
            {
                var idx = token.IndexOf('=');
                if (idx <= 0) continue;
                var key = token[..idx].Trim().ToLowerInvariant();
                var val = token[(idx + 1)..].Trim();
                if (key.Length > 0)
                {
                    map[key] = val;
                }
            }
        }

        if (!map.ContainsKey("race_no"))
        {
            var match = System.Text.RegularExpressions.Regex.Match(text, @"(?<!\d)(\d{1,2})R(?![A-Za-z])", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            if (match.Success)
            {
                map["race_no"] = match.Groups[1].Value;
            }
        }

        if (!map.ContainsKey("race_date"))
        {
            var m = System.Text.RegularExpressions.Regex.Match(text, @"(20\d{6})");
            if (m.Success)
            {
                map["race_date"] = m.Groups[1].Value;
            }
        }

        return map;
    }
}

internal static class FixedWidthJVParser
{
    private static readonly Encoding ShiftJis = Encoding.GetEncoding(932);

    public static bool IsLikelyFixedWidth(string text)
    {
        return text.Length >= 200;
    }

    public static Dictionary<string, string> Parse(string type, string text)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var bytes = ShiftJis.GetBytes(text);
        switch (type)
        {
            case "RA":
                ParseRa(map, bytes, text);
                break;
            case "SE":
                ParseSe(map, bytes);
                break;
            case "HR":
                ParseHr(map, bytes);
                break;
            case "O3":
                ParseO3(map, bytes);
                break;
            case "O1":
                ParseO1(map, bytes);
                break;
            case "O2":
                ParseO2(map, bytes);
                break;
        }
        return map;
    }

    private static void ParseO3(Dictionary<string, string> map, byte[] bytes)
    {
        // O3: odds3 (wide)
        // Positions based on JV-Data spec (byte positions, 1-based):
        // - held year: 12 (4), held md: 16 (4) => kaisai_date yyyymmdd
        // - venue_code: 20 (2)
        // - race_no: 26 (2)
        var year = Slice(bytes, 12, 4);
        var md = Slice(bytes, 16, 4);
        var kaisaiDate = (year + md).Trim();
        if (kaisaiDate.Length == 8)
        {
            map["kaisai_date"] = kaisaiDate;
            map["race_date"] = kaisaiDate;
        }
        map["venue_code"] = Slice(bytes, 20, 2);
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));
    }

    private static void ParseO1(Dictionary<string, string> map, byte[] bytes)
    {
        // O1: odds1 (win/place/bracket)
        var year = Slice(bytes, 12, 4);
        var md = Slice(bytes, 16, 4);
        var kaisaiDate = (year + md).Trim();
        if (kaisaiDate.Length == 8)
        {
            map["kaisai_date"] = kaisaiDate;
            map["race_date"] = kaisaiDate;
        }
        map["venue_code"] = Slice(bytes, 20, 2);
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));
    }

    private static void ParseO2(Dictionary<string, string> map, byte[] bytes)
    {
        // O2: odds2 (umaren)
        var year = Slice(bytes, 12, 4);
        var md = Slice(bytes, 16, 4);
        var kaisaiDate = (year + md).Trim();
        if (kaisaiDate.Length == 8)
        {
            map["kaisai_date"] = kaisaiDate;
            map["race_date"] = kaisaiDate;
        }
        map["venue_code"] = Slice(bytes, 20, 2);
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));
    }

    private static void ParseRa(Dictionary<string, string> map, byte[] bytes, string text)
    {
        var yyyy = Slice(bytes, 12, 4);
        var mmdd = Slice(bytes, 16, 4);
        map["race_date"] = yyyy + mmdd;
        map["kaisai_date"] = yyyy + mmdd;
        map["venue_code"] = Slice(bytes, 20, 2);
        map["jyo_cd"] = map["venue_code"];
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));

        var nameMain = Slice(bytes, 33, 60).Trim();
        var nameSub = Slice(bytes, 93, 60).Trim();
        var nameInner = Slice(bytes, 153, 60).Trim();
        var raceName = string.Join(" ", new[] { nameMain, nameSub, nameInner }.Where(x => x.Length > 0));
        map["race_name"] = raceName;
        map["name"] = raceName;

        map["distance"] = TrimLeadingZeros(Slice(bytes, 698, 4));
        map["kyori"] = map["distance"];
        map["track_code"] = Slice(bytes, 706, 2);
        map["surface"] = ToSurface(map["track_code"]);
        map["field_size"] = TrimLeadingZeros(Slice(bytes, 884, 2));
        map["head_count"] = map["field_size"];
        map["grade"] = Slice(bytes, 615, 1);

        // JV-Link COM returns decoded strings, not original bytes. After Japanese text fields,
        // byte-based offsets can drift in this environment, so recover key race metadata from
        // stable character positions observed in RA records.
        var distanceFromText = ParseIntOrNull(SliceText(text, 698, 4));
        if (distanceFromText.HasValue && distanceFromText.Value is >= 800 and <= 4300)
        {
            map["distance"] = distanceFromText.Value.ToString(CultureInfo.InvariantCulture);
            map["kyori"] = map["distance"];
        }

        var fieldSizeFromText = ParseIntOrNull(SliceText(text, 882, 2));
        if (fieldSizeFromText.HasValue && fieldSizeFromText.Value is >= 1 and <= 18)
        {
            map["field_size"] = fieldSizeFromText.Value.ToString(CultureInfo.InvariantCulture);
            map["head_count"] = map["field_size"];
        }

        var trackFromText = SliceText(text, 710, 1).Trim();
        if (!string.IsNullOrWhiteSpace(trackFromText))
        {
            map["track_code"] = trackFromText;
            map["surface"] = ToSurface(trackFromText);
        }
    }

    private static void ParseSe(Dictionary<string, string> map, byte[] bytes)
    {
        var yyyy = Slice(bytes, 12, 4);
        var mmdd = Slice(bytes, 16, 4);
        map["race_date"] = yyyy + mmdd;
        map["kaisai_date"] = yyyy + mmdd;
        map["venue_code"] = Slice(bytes, 20, 2);
        map["jyo_cd"] = map["venue_code"];
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));
        map["waku"] = TrimLeadingZeros(Slice(bytes, 28, 1));
        map["umaban"] = TrimLeadingZeros(Slice(bytes, 29, 2));
        map["horse_id"] = Slice(bytes, 31, 10);
        map["ketto_num"] = map["horse_id"];
        map["horse_name"] = Slice(bytes, 41, 36);
        map["bamei"] = map["horse_name"];
        map["trainer_name"] = Slice(bytes, 91, 8);
        map["chokyoshi_name"] = map["trainer_name"];
        map["jockey_name"] = Slice(bytes, 307, 8);
        map["kishu_name"] = map["jockey_name"];

        var odds10 = ParseIntOrNull(Slice(bytes, 360, 4));
        if (odds10.HasValue)
        {
            map["odds"] = (odds10.Value / 10m).ToString(CultureInfo.InvariantCulture);
            map["tansho_odds"] = map["odds"];
        }

        map["finish_pos"] = TrimLeadingZeros(Slice(bytes, 335, 2));
        map["chakujun"] = map["finish_pos"];
        map["corner1_pos"] = TrimLeadingZeros(Slice(bytes, 352, 2));
        map["corner2_pos"] = TrimLeadingZeros(Slice(bytes, 354, 2));
        map["corner3_pos"] = TrimLeadingZeros(Slice(bytes, 356, 2));
        map["corner4_pos"] = TrimLeadingZeros(Slice(bytes, 358, 2));
        var last3f10 = ParseIntOrNull(Slice(bytes, 391, 3));
        if (last3f10.HasValue)
        {
            map["last3f"] = (last3f10.Value / 10m).ToString(CultureInfo.InvariantCulture);
            map["agari3f"] = map["last3f"];
        }
        map["race_time"] = Slice(bytes, 339, 4);
        map["time"] = map["race_time"];
    }

    private static void ParseHr(Dictionary<string, string> map, byte[] bytes)
    {
        var yyyy = Slice(bytes, 12, 4);
        var mmdd = Slice(bytes, 16, 4);
        map["race_date"] = yyyy + mmdd;
        map["kaisai_date"] = yyyy + mmdd;
        map["venue_code"] = Slice(bytes, 20, 2);
        map["jyo_cd"] = map["venue_code"];
        map["kai_day"] = TrimLeadingZeros(Slice(bytes, 24, 2));
        map["nichiji"] = map["kai_day"];
        map["race_no"] = TrimLeadingZeros(Slice(bytes, 26, 2));

        AddPayouts(map, bytes, "TAN", 103, 13, 13, 2, 9);
        AddPayouts(map, bytes, "FUKU", 142, 13, 5, 2, 9);
        AddPayouts(map, bytes, "WAKU", 207, 13, 3, 4, 9);
        AddPayouts(map, bytes, "UMAREN", 246, 16, 3, 4, 9);
        AddPayouts(map, bytes, "WIDE", 294, 16, 7, 4, 9);
        AddPayouts(map, bytes, "UMATAN", 454, 16, 6, 4, 9);
        AddPayouts(map, bytes, "SANRENPUKU", 550, 18, 3, 6, 9);
        AddPayouts(map, bytes, "SANRENTAN", 604, 19, 6, 6, 9);
    }

    private static void AddPayouts(Dictionary<string, string> map, byte[] bytes, string betType, int start, int unitLength, int count, int comboLength, int payoutLength)
    {
        for (var i = 0; i < count; i++)
        {
            var offset = start + i * unitLength;
            var comboRaw = Slice(bytes, offset, comboLength);
            var payoutRaw = Slice(bytes, offset + comboLength, payoutLength);
            var payout = ParseIntOrNull(payoutRaw);
            if (!payout.HasValue || payout.Value <= 0)
            {
                continue;
            }

            var combo = NormalizeCombo(comboRaw);
            if (combo.Length == 0 || combo.All(c => c == '0' || c == '-'))
            {
                continue;
            }

            map[$"{betType}_{combo}"] = payout.Value.ToString(CultureInfo.InvariantCulture);
        }
    }

    private static string NormalizeCombo(string raw)
    {
        var digits = new string((raw ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 2)
        {
            return digits;
        }
        if (digits.Length == 4)
        {
            return $"{digits[..2]}-{digits.Substring(2, 2)}";
        }
        if (digits.Length == 6)
        {
            return $"{digits[..2]}-{digits.Substring(2, 2)}-{digits.Substring(4, 2)}";
        }
        return digits;
    }

    private static string Slice(byte[] bytes, int start1, int length)
    {
        if (length <= 0 || start1 <= 0)
        {
            return string.Empty;
        }

        var start = start1 - 1;
        if (start >= bytes.Length)
        {
            return string.Empty;
        }

        var len = Math.Min(length, bytes.Length - start);
        var segment = ShiftJis.GetString(bytes, start, len);
        return segment.TrimEnd('\0').Trim();
    }

    private static string SliceText(string text, int start1, int length)
    {
        if (length <= 0 || start1 <= 0 || string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        var start = start1 - 1;
        if (start >= text.Length)
        {
            return string.Empty;
        }

        var len = Math.Min(length, text.Length - start);
        return text.Substring(start, len).TrimEnd('\0').Trim();
    }

    private static string TrimLeadingZeros(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }
        var digits = new string(value.Trim().Where(char.IsDigit).ToArray());
        if (digits.Length == 0)
        {
            return value.Trim();
        }
        return int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var x)
            ? x.ToString(CultureInfo.InvariantCulture)
            : digits;
    }

    private static int? ParseIntOrNull(string value)
    {
        var digits = new string((value ?? string.Empty).Where(c => char.IsDigit(c) || c == '-' || c == '+').ToArray());
        if (digits.Length == 0)
        {
            return null;
        }
        return int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var x) ? x : null;
    }

    private static string ToSurface(string trackCode)
    {
        var code = (trackCode ?? string.Empty).Trim();
        if (code.Length == 0)
        {
            return string.Empty;
        }
        return code[0] switch
        {
            '1' => "芝",
            '2' => "ダ",
            '3' => "障",
            _ => code,
        };
    }
}

internal sealed class RawManifest
{
    [JsonPropertyName("race_date")]
    public string RaceDate { get; init; } = "";

    [JsonPropertyName("generated_at")]
    public string GeneratedAt { get; init; } = "";

    [JsonPropertyName("source")]
    public string Source { get; init; } = "jvlink_sdk_direct";

    [JsonPropertyName("has_races")]
    public bool HasRaces { get; init; }

    [JsonPropertyName("has_entries")]
    public bool HasEntries { get; init; }

    [JsonPropertyName("has_results")]
    public bool HasResults { get; init; }

    [JsonPropertyName("has_payouts")]
    public bool HasPayouts { get; init; }

    [JsonPropertyName("row_counts")]
    public Dictionary<string, int> RowCounts { get; init; } = new();

    [JsonPropertyName("missing_columns")]
    public Dictionary<string, List<string>> MissingColumns { get; init; } = new();

    [JsonPropertyName("warnings")]
    public List<string> Warnings { get; init; } = [];

    [JsonPropertyName("jvopen_failures")]
    public List<string> JVOpenFailures { get; init; } = [];

    [JsonPropertyName("args")]
    public Dictionary<string, string> Args { get; init; } = new();

    [JsonPropertyName("jvopen_attempts")]
    public List<JVOpenAttempt> JVOpenAttempts { get; init; } = [];

    [JsonPropertyName("jvopen_dataspec")]
    public string? JVOpenDataSpec { get; init; }

    [JsonPropertyName("jvopen_fromtime")]
    public string? JVOpenFromTime { get; init; }

    [JsonPropertyName("jvopen_option")]
    public string? JVOpenOption { get; init; }

    [JsonPropertyName("read_record_type_counts")]
    public Dictionary<string, int> ReadRecordTypeCounts { get; init; } = new();

    [JsonPropertyName("record_type_counts")]
    public Dictionary<string, int> RecordTypeCounts { get; init; } = new();

    [JsonPropertyName("return_code")]
    public int? ReturnCode { get; init; }

    [JsonPropertyName("return_code_meaning")]
    public string ReturnCodeMeaning { get; init; } = "unknown";

    [JsonPropertyName("read_count")]
    public int? ReadCount { get; init; }

    [JsonPropertyName("download_count")]
    public int? DownloadCount { get; init; }

    [JsonPropertyName("read_errors")]
    public List<ReadErrorEntry> ReadErrors { get; init; } = [];

    [JsonPropertyName("skipped_files")]
    public List<string> SkippedFiles { get; init; } = [];

    [JsonPropertyName("read_retry_count")]
    public int ReadRetryCount { get; init; }

    [JsonPropertyName("read_retry_sleep_sec")]
    public int ReadRetrySleepSec { get; init; }

    [JsonPropertyName("hr_record_count")]
    public int HrRecordCount { get; init; }

    [JsonPropertyName("date_min")]
    public string? DateMin { get; init; }

    [JsonPropertyName("date_max")]
    public string? DateMax { get; init; }

    public static RawManifest Build(
        CliOptions options,
        Dictionary<string, int> rowCounts,
        IReadOnlyCollection<string> warnings,
        Dictionary<string, List<string>> missingColumns,
        IReadOnlyCollection<JVOpenAttempt>? jvopenAttempts = null,
        IReadOnlyDictionary<string, int>? readRecordTypeCounts = null,
        int hrRecordCount = 0,
        string? dateMin = null,
        string? dateMax = null,
        IReadOnlyCollection<ReadErrorEntry>? readErrors = null,
        IReadOnlyCollection<string>? skippedFiles = null,
        int readRetryCount = 0,
        int readRetrySleepSec = 0)
    {
        var attempts = jvopenAttempts?.ToList() ?? [];
        var latestAttempt = attempts.LastOrDefault();
        var typeCounts = readRecordTypeCounts is null
            ? new Dictionary<string, int>()
            : new Dictionary<string, int>(readRecordTypeCounts, StringComparer.OrdinalIgnoreCase);
        return new RawManifest
        {
            RaceDate = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            GeneratedAt = DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture),
            HasRaces = rowCounts.GetValueOrDefault("races") > 0,
            HasEntries = rowCounts.GetValueOrDefault("entries") > 0,
            HasResults = rowCounts.GetValueOrDefault("results") > 0,
            HasPayouts = rowCounts.GetValueOrDefault("payouts") > 0,
            RowCounts = rowCounts,
            MissingColumns = missingColumns,
            Warnings = warnings.ToList(),
            JVOpenFailures = warnings.Where(w => w.StartsWith("jvopen_failed:", StringComparison.OrdinalIgnoreCase)).ToList(),
            Args = new Dictionary<string, string>
            {
                ["race_date"] = options.RaceDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                ["output_dir"] = options.OutputDir.FullName,
                ["dataspec"] = options.DataSpec,
                ["fromtime"] = options.FromTime,
                ["option"] = options.Option,
                ["debug_jvopen"] = options.DebugJVOpen.ToString(),
                ["setup_mode"] = options.SetupMode.ToString(),
                ["dry_run"] = options.DryRun.ToString(),
            },
            JVOpenAttempts = attempts,
            JVOpenDataSpec = options.SetupMode ? "RACE" : options.DataSpec,
            JVOpenFromTime = options.FromTime,
            JVOpenOption = options.Option,
            ReadRecordTypeCounts = typeCounts,
            RecordTypeCounts = typeCounts,
            ReturnCode = latestAttempt?.ReturnCode,
            ReturnCodeMeaning = latestAttempt?.ReturnCodeMeaning ?? "unknown",
            ReadCount = latestAttempt?.ReadCount,
            DownloadCount = latestAttempt?.DownloadCount,
            HrRecordCount = hrRecordCount,
            DateMin = dateMin ?? "",
            DateMax = dateMax ?? "",
            ReadErrors = readErrors?.ToList() ?? [],
            SkippedFiles = skippedFiles?.ToList() ?? [],
            ReadRetryCount = readRetryCount,
            ReadRetrySleepSec = readRetrySleepSec,
        };
    }
}

internal sealed class JVOpenAttempt
{
    [JsonPropertyName("dataspec")]
    public string DataSpec { get; init; } = "";

    [JsonPropertyName("fromtime")]
    public string FromTime { get; init; } = "";

    [JsonPropertyName("option")]
    public int Option { get; init; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    [JsonPropertyName("return_code")]
    public int? ReturnCode { get; set; }

    [JsonPropertyName("return_code_meaning")]
    public string ReturnCodeMeaning { get; set; } = "unknown";

    [JsonPropertyName("read_count")]
    public int? ReadCount { get; set; }

    [JsonPropertyName("download_count")]
    public int? DownloadCount { get; set; }
}

internal sealed class ReadErrorEntry
{
    [JsonPropertyName("return_code")]
    public int ReturnCode { get; init; }

    [JsonPropertyName("return_code_meaning")]
    public string ReturnCodeMeaning { get; init; } = "unknown";

    [JsonPropertyName("file_name")]
    public string FileName { get; init; } = "";

    [JsonPropertyName("record_count")]
    public int RecordCount { get; init; }

    [JsonPropertyName("elapsed_sec")]
    public double ElapsedSec { get; init; }
}

internal sealed class RaceRow
{
    public string RaceId { get; init; } = "";
    public string RaceDate { get; init; } = "";
    public string VenueCode { get; init; } = "";
    public string Venue { get; init; } = "";
    public int RaceNo { get; init; }
    public string RaceName { get; init; } = "";
    public int? Distance { get; init; }
    public string Surface { get; init; } = "";
    public string TrackCondition { get; init; } = "";
    public int? FieldSize { get; init; }
    public string Grade { get; init; } = "";
}

internal sealed class EntryRow
{
    public string RaceId { get; init; } = "";
    public string HorseId { get; init; } = "";
    public string HorseName { get; init; } = "";
    public int? Umaban { get; init; }
    public int? Waku { get; init; }
    public string JockeyName { get; init; } = "";
    public string TrainerName { get; init; } = "";
    public decimal? WeightCarried { get; init; }
    public decimal? Odds { get; init; }
    public int? Popularity { get; init; }
}

internal sealed class ResultRow
{
    public string RaceId { get; init; } = "";
    public string HorseId { get; init; } = "";
    public int? Umaban { get; init; }
    public int? FinishPos { get; init; }
    public string Time { get; init; } = "";
    public decimal? Margin { get; init; }
    public int? Corner1Pos { get; init; }
    public int? Corner2Pos { get; init; }
    public int? Corner3Pos { get; init; }
    public int? Corner4Pos { get; init; }
    public decimal? Last3F { get; init; }
    public decimal? Odds { get; init; }
    public int? Popularity { get; init; }
}

internal sealed class PayoutRow
{
    public string RaceId { get; init; } = "";
    public string BetType { get; init; } = "";
    public string WinningCombination { get; init; } = "";
    public int? PayoutYen { get; init; }
}

internal sealed class OddsRow
{
    public string RaceId { get; init; } = "";
    public string OddsSnapshotVersion { get; init; } = "";
    public string CapturedAt { get; init; } = "";
    public string OddsType { get; init; } = "";
    public int HorseNo { get; init; }
    public int HorseNoA { get; init; }
    public int HorseNoB { get; init; }
    public decimal? OddsValue { get; init; }
    public string SourceVersion { get; init; } = "";
}

internal static class ComIntrospection
{
    public static void PrintDispatchMembers(object comObject, Logger logger)
    {
        logger.Info("Enumerating COM IDispatch members (names only)...");
        IntPtr dispatchPtr = IntPtr.Zero;
        try
        {
            dispatchPtr = Marshal.GetIDispatchForObject(comObject);
            var dispatch = (IDispatch)Marshal.GetObjectForIUnknown(dispatchPtr);
            dispatch.GetTypeInfo(0, 0, out var typeInfo);
            var names = GetAllFunctionNames(typeInfo);
            logger.Info($"COM members found: {names.Count}");
            foreach (var name in names.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine(name);
            }
        }
        finally
        {
            if (dispatchPtr != IntPtr.Zero)
            {
                Marshal.Release(dispatchPtr);
            }
        }
    }

    private static HashSet<string> GetAllFunctionNames(ITypeInfo typeInfo)
    {
        typeInfo.GetTypeAttr(out var pTypeAttr);
        var attr = Marshal.PtrToStructure<TYPEATTR>(pTypeAttr);
        var outNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            for (var i = 0; i < attr.cFuncs; i++)
            {
                typeInfo.GetFuncDesc(i, out var pFuncDesc);
                try
                {
                    var funcDesc = Marshal.PtrToStructure<FUNCDESC>(pFuncDesc);
                    var rg = new string[funcDesc.cParams + 1];
                    typeInfo.GetNames(funcDesc.memid, rg, rg.Length, out var fetched);
                    if (fetched > 0 && !string.IsNullOrWhiteSpace(rg[0]))
                    {
                        outNames.Add(rg[0]);
                    }
                }
                finally
                {
                    typeInfo.ReleaseFuncDesc(pFuncDesc);
                }
            }
        }
        finally
        {
            typeInfo.ReleaseTypeAttr(pTypeAttr);
        }
        return outNames;
    }

    [ComImport]
    [Guid("00020400-0000-0000-C000-000000000046")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IDispatch
    {
        void GetTypeInfoCount(out uint pctinfo);
        void GetTypeInfo(uint iTInfo, uint lcid, out ITypeInfo ppTInfo);
        void GetIDsOfNames(ref Guid riid, IntPtr rgszNames, uint cNames, uint lcid, IntPtr rgDispId);
        void Invoke(int dispIdMember, ref Guid riid, uint lcid, ushort wFlags, IntPtr pDispParams, IntPtr pVarResult, IntPtr pExcepInfo, IntPtr puArgErr);
    }
}

