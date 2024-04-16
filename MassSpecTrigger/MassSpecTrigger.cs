
/*
 * MassSpecTrigger: Program to copy or move RAW files from ThermoFisher mass spectrometers.
 * Intended to be used as part of an automation pipeline.
 * Will be called from Xcalibur after each RAW file is created.
 * In Xcalibur post-processing dialog, specify the complete path to the MassSpecTrigger
 * executable, followed by the %R variable to supply the path to the new RAW file. The
 * Xcalibur sequence (SLD) file should be created in the same directory as the RAW files
 * and there should only be a single SLD file per directory.
 * 
 * When all RAW files for a sequence have been produced, it will:
 * - Copy or move them to a destination folder (depending on config file)
 * - Create a trigger file (MSAComplete.txt) in the destination folder
 *
 * Argument #1: the current RAW file (%R from Xcalibur post-processing dialog)
 */

/* 
Rules:
Yes, patients do get re-processed in three scenarios:
a) Patient samples are run on a different instrument and a new folder is created with updated instrument name in the folder name

b) Patient samples are repeated either on same instrument or different instrument. 
For this, a tag of "_RPTMS" is put in the file names. If all samples are repeated, there would be a new folder with tag "_RPTMS". 
But, if only some are repeated, the new files are kept in the same folder and combined with other files for processing and
c) patient biopsy gets a fresh microdissection. This is handled the same way as scenario b with exception of using tag "_RPTLMD"

common: "_RPT*"
---
New requirements 10/2023
When Xcalibur triggers the executable that creates the MSAComplete.txt file, 
it should also move the resulting RAW files over to the NAS drive. 
This will be a network share on the Windows machine, probably mapped to a drive letter.

To avoid adding more complexity to the watcher script, we think adding a step to the MassSpecTrigger code to handle this step makes the most sense. 
A plain-text config file in the same directory as the executable will define the input and output directories, and 
the code will move everything from the input directory to the output directory, like the robocopy command below.

If the executable and config file are placed on the D: drive, this will be maintainable since we can edit the config file via the shared directory on the NAS.
(In all cases, make sure to move MSAComplete.txt file last, after all RAW files have been moved.)

formerly Robocopy command:  robocopy D:\Transfer "Z:\Transfer\" *.raw /min:<100000> /MOV /Z /S /XO /R:3 /W:2000 /MINAGE:.01
/S : Copy Subfolders.   [ok]
/R:3 : 3 Retries on failed copies. [NA]
/W:2000 : Wait time between retries IN seconds. [NA]
/MOV : MOVe files (delete from source after copying). [ok]
/Z : Copy files in restartable mode (survive network glitch) use with caution as
                     this significantly reduces copy performance due to the extra logging. [NA]
/XO : Exclude Older - if destination file already exists and is the same date or newer than the source, don�t overwrite it. [does not overwrite if exists]
/MINAGE:.01 DAYS: MINimum file AGE - exclude files newer than n days/date. [NA]
*/

using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Collections;
using System.Collections.Generic;
using System.Xml;
using System.Linq;
using Color = System.Drawing.Color;
using System.Collections.Specialized;
using System.Reflection;
using System.Resources;
using System.Globalization;
// using Microsoft.Toolkit.Uwp.Notifications;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using CommandLine;
using CommandLine.Text;
using Pastel;

namespace MassSpecTrigger
{
    public class Options
    {
        [Option('m', "mock", Required = false, MetaValue = "\"file1.raw;file2.raw;...\"", HelpText = "Mock sequence: a semicolon-separated list of RAW files (complete path) which will stand in for the contents of an SLD file")]
        public string MockSequence { get; set; }

        [Option('l', "logfile", Required = false, MetaValue = "\"logfile\"", HelpText = "Complete path to log file")]
        public string Logfile { get; set; }

        [Option('n', "notification", Required = false, HelpText = "Send a test error notification and exit")]
        public bool TestNotifications { get; set; }

        [Option('d', "debug", Required = false, HelpText = "Enable debug output")]
        public bool Debug { get; set; }
        
        [Value(0, MetaName = "\"file_path.raw\"", HelpText = "RAW file (complete path)")]
        public string InputRawFile { get; set; } 
    }

    // [AttributeUsage(AttributeTargets.Assembly)]
    // internal class BuildDateAttribute : Attribute
    // {
    //     public BuildDateAttribute(string value)
    //     {
    //         DateTime = DateTime.ParseExact(
    //             value, "yyyyMMddHHmmss",
    //             CultureInfo.InvariantCulture,
    //             DateTimeStyles.None);
    //     }
    //
    //     public DateTime DateTime { get; }
    // }

    // to avoid most casting
    public class StringKeyDictionary : OrderedDictionary, IEnumerable<KeyValuePair<string, object>>
    {
        public object this[string key]
        {
            get => base[key];
            set
            {
                ValidateValueType(value);
                base[key] = value;
            }
        }

        public void Add(string key, object value)
        {
            ValidateValueType(value);
            base.Add(key, value);
        }

        public void Insert(int index, string key, object value)
        {
            ValidateValueType(value);
            base.Insert(index, key, value);
        }

        private void ValidateValueType(object value)
        {
            if (value is not int && value is not bool && value is not string)
            {
                throw new ArgumentException("Value must be of type int, bool, or string.", nameof(value));
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public new IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            foreach (DictionaryEntry entry in (IEnumerable)base.GetEnumerator())
            {
                if (entry.Key is string key)
                {
                    yield return new KeyValuePair<string, object>(key, entry.Value);
                }
                else
                {
                    throw new InvalidOperationException("Invalid key or value type detected.");
                }
            }
        }

        public bool TryGetValue(string key, out string outputValue)
        {
            if (this.Contains(key))
            {
                outputValue = (string)this[key];
                return true;
            }
            else
            {
                outputValue = "";
                return false;
            }
        }
        
        public object TryGetValue(string key, out int outputValue)
        {
            if (this.Contains(key))
            {
                outputValue = (int)this[key];
                return true;
            }
            else
            {
                outputValue = 0;
                return false;
            }
        }
    
        public object TryGetValue(string key, out bool outputValue)
        {
            if (this.Contains(key))
            {
                outputValue = (bool)this[key];
                return true;
            }
            else
            {
                outputValue = false;
                return false;
            }
        }

        public object GetValueOrDefault(string key, object defaultValue)
        {
            if (this.Contains(key))
            {
                return this[key];
            }
            else
            {
                return defaultValue;
            }
        }

        public string GetValueOrDefault(string key, string defaultValue)
        {
            return (string)GetValueOrDefault(key, (object)defaultValue);
        }

        public int GetValueOrDefault(string key, int defaultValue)
        {
            return (int)GetValueOrDefault(key, (object)defaultValue);
        }
    
        public bool GetValueOrDefault(string key, bool defaultValue)
        {
            return (bool)GetValueOrDefault(key, (object)defaultValue);
        }
    }  // StringKeyDictionary

    public class StringOrderedDictionary : OrderedDictionary
    {
        public string this[string key]
        {
            get => base[key].ToString();
            set => base[key] = (string)value;
        }
    }  // StringOrderedDictionary

    public static class MainClass
    {
        public static string AppName = Assembly.GetExecutingAssembly().GetName().Name;
        public static string AppVersion = Assembly.GetExecutingAssembly().GetName().Version?.ToString(); 
        public const string LinuxEndl = "\n";
        public const string RawFilePattern = ".raw";
        public const string OutputDirKey = "Output_Directory";
        public const string RepeatRunKey = "Repeat_Run_Matches";
        public const string TokenFileKey = "Token_File";
        public const string FailureTokenFileKey = "Failure_Token_File";
        public const string SourceTrimKey = "Source_Trim";
        public const string SldStartsWithKey = "SLD_Starts_With";
        public const string PostBlankMatchesKey = "PostBlank_Matches";
        public const string IgnorePostBlankKey = "Ignore_PostBlank";
        public const string RemoveFilesKey = "Remove_Files";
        public const string RemoveDirectoriesKey = "Remove_Directories";
        public const string PreserveSldKey = "Preserve_SLD";
        public const string UpdateFilesKey = "Overwrite_Older";
        public const string MinRawFileSizeKey = "Min_Raw_Files_To_Move_Again";
        public const string DebugKey = "Debug";
        public const string TriggerLogFileStem = "mass_spec_trigger_log_file";
        public const string DefaultConfigFilename = "MassSpecTrigger.cfg";
        public const string TriggerLogFileExtension = "txt";
        public static string DefaultSourceTrim = "Transfer";
        public static string DefaultRepeatRun = "_RPT";
        public static string DefaultTokenFile = "MSAComplete.txt";
        public static string DefaultFailureTokenFile = "MSAFailure.txt";
        public static string DefaultSldStartsWith = "Exploris";
        public static string DefaultPostBlankMatches = "PostBlank";
        public static bool DefaultIgnorePostBlank = true;
        public static bool DefaultRemoveFiles = false;
        public static bool DefaultRemoveDirectories = false;
        public static bool DefaultPreserveSld = true;
        public static bool DefaultUpdateFiles = false;
        public static bool DefaultDebugging = false;
        public const int DefaultMinRawFileSize = 100000;
        public static string SourceTrim = DefaultSourceTrim;
        public static string RepeatRun = DefaultRepeatRun;
        public static string TokenFile = DefaultTokenFile;
        public static string FailureTokenFile = DefaultFailureTokenFile;
        public static string SldStartsWith = DefaultSldStartsWith;
        public static string PostBlankMatches = DefaultPostBlankMatches;
        public static bool IgnorePostBlank = DefaultIgnorePostBlank;
        public static bool RemoveFiles = DefaultRemoveFiles;
        public static bool RemoveDirectories = DefaultRemoveDirectories;
        public static bool PreserveSld = DefaultPreserveSld;
        public static bool UpdateFiles = DefaultUpdateFiles;
        public static bool DebugMode = DefaultDebugging;
        public static int MinRawFileSize = DefaultMinRawFileSize;
        public static StringKeyDictionary ConfigMap;

        public static StreamWriter logFile;
        public static bool MockSequenceMode = false;

        private const string RAW_FILES_ACQUIRED_BASE = "RawFilesAcquired.txt";
        public static string SLD_FILE_PATH = "";
        public static string rawFileName = "";
        public static string resourceFolderPath = "";
        public static string logFolderPath = "";
        public static string logFilePath = "";
        public static List<string> mockSequence = new List<string>();
        public static List<string> tempDirectories = new List<string>();

        public static Parser parser;
        public static ParserResult<Options> parserResult;

        public static void log(string message, StreamWriter logger = null)
        {
            /* Use default logFile if logger not provided */
            logger ??= logFile;
            var ts = Timestamp();
            var msg = $"[ {ts} ] {message}";
            logger?.WriteLine(msg);
            msg = msg.Pastel(Color.Cyan);
            /* Write to stderr in case we need to use output for something */
            Console.Error.WriteLine(msg);
        }

        public static void logerr(string message, StreamWriter logger = null)
        {
            /* Use default logFile if logger not provided */
            logger ??= logFile;
            var ts = Timestamp();
            var line = $"[ {ts} ] [ERROR] {message}";
            logger?.WriteLine(line);
            line = line.Pastel(Color.Red);
            Console.Error.WriteLine(line);
        }

        public static void logwarn(string message, StreamWriter logger = null)
        {
            /* Use default logFile if logger not provided */
            logger ??= logFile;
            var ts = Timestamp();
            var line = $"[ {ts} ] [WARNING] {message}";
            logger?.WriteLine(line);
            line = line.Pastel(Color.Yellow);
            Console.Error.WriteLine(line);
        }

        public static void logdbg(string message, StreamWriter logger = null)
        {
            /* Use default logFile if logger not provided */
            logger ??= logFile;
            if (DebugMode)
            {
                var ts = Timestamp();
                var line = $"[ {ts} ] [DEBUG] {message}";
                logger?.WriteLine(line);
                line = line.Pastel(Color.Magenta);
                Console.Error.WriteLine(line);
            }
        }

        public static string StringifyDictionary(StringOrderedDictionary dict)
        {
            string output = "";
            if (dict is null)
            {
                output += "(null)\n";
            }
            else if (dict.Count == 0)
            {
                output += "(empty)\n";
            }
            else
            {
                foreach (DictionaryEntry de in dict)
                {
                    output += $"{de.Key}={de.Value}\n";
                }
            }
            return output.Trim();
        }
        
        public static bool ContainsCaseInsensitiveSubstring(string str, string substr)
        {
            string strLower = str.ToLower();
            string substrLower = substr.ToLower();
            return strLower.Contains(substrLower);
        }

        public static string Timestamp(string format)
        {
            DateTime now = DateTime.Now;
            return now.ToString(format);
        }

        public static string Timestamp() => Timestamp("yyyy-MM-dd HH:mm:ss");

        public static string TriggerFileTimestamp() => Timestamp("yyyy_M_d_H_m_s");


        // public static DateTime GetBuildDate(Assembly assembly)
        // {
        //     var attribute = assembly.GetCustomAttribute<BuildDateAttribute>();
        //     return attribute?.DateTime ?? default(DateTime);
        //     // ResourceManager rm = new ResourceManager("Strings", typeof(MainClass).Assembly);
        //     // string buildTimestamp = rm.GetString("BuildTimestamp");
        // }

        public static string GetBuildTimestamp()
        {
            // var utcBuildTime = GetBuildDate(Assembly.GetExecutingAssembly());
            var buildTime = File.GetLastWriteTime(Assembly.GetExecutingAssembly().Location);
            // Console.WriteLine($"buildTime: {buildTime}");
            return buildTime.ToString("yyyy-MM-ddTHH:mm:sszzz");
        }

        public static bool IsTempSldFile(string filepath)
        {
            string filename = Path.GetFileName(filepath);
            Regex tempSldRegex = new Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}.sld$", RegexOptions.IgnoreCase);
            return tempSldRegex.IsMatch(filename);
        } // IsTempSldFile()
        
        public static string ConstructDestinationPath(string sourceDir, string outputDir, string sourceTrimPath = "")
        {
            string sourceStr = sourceDir.Replace(Path.GetPathRoot(sourceDir) ?? string.Empty, "");
            int sourceTrimPos = -1;
            // Don't trim anything if sourceTrimPath is an empty string
            if (!string.IsNullOrEmpty(sourceTrimPath)) 
            {
                sourceTrimPos = sourceStr.IndexOf(sourceTrimPath, StringComparison.OrdinalIgnoreCase);
            }
            string newOutputPath = "";
            if (sourceTrimPos == -1)
            {
                newOutputPath = Path.Combine(outputDir, sourceStr);
            }
            else if (sourceTrimPos == 0 && sourceStr == sourceTrimPath)
            {
                // If source path is identical to sourceTrimPath, just save directly in output directory 
                newOutputPath = outputDir;
            }
            else
            {
                // If string matching contents of sourceTrimPath is found, adjust the substring start position.
                // Add 1 to include the slash after the directory so it doesn't append as a root directory.
                if (sourceTrimPath != null)
                {
                    int startSubstrPos = (sourceTrimPos + sourceTrimPath.Length + 1);
                    string newRelativePath = sourceStr.Substring(startSubstrPos);
                    newOutputPath = Path.Combine(outputDir, newRelativePath);
                }
            }
            return newOutputPath;
        }

        public static bool CreateOutputDirectory(string outputPath)
        {
            if (!string.IsNullOrEmpty(outputPath))
            {
                if (!Directory.Exists(outputPath))
                {
                    Directory.CreateDirectory(outputPath);
                }
                return true;
            }
            else
            {
                logerr($"CreateOutputDirectory called with empty path");
            }

            return false;
        } // CreateOutputDirectory()

        public static bool PrepareOutputDirectory(string outputPath, int minRawFileSize, string filePattern)
        {
            if (!string.IsNullOrEmpty(outputPath))
            {
                CreateOutputDirectory(outputPath);
                if (!Directory.Exists(outputPath))
                {
                    Directory.CreateDirectory(outputPath);
                    return true;
                }
                var matchingFiles = Directory.GetFiles(outputPath, "*" + filePattern)
                    .Where(filePath => new FileInfo(filePath).Length < minRawFileSize)
                    .ToList();
                if (matchingFiles.Count > 0)
                {
                    log("Found existing small raw files, previous copy may have been interrupted. Deleting " + outputPath);
                    Directory.Delete(outputPath, true);
                    CreateOutputDirectory(outputPath);
                }
                return true;
            }
            return false;
        } // PrepareOutputDirectory()

        public static StringOrderedDictionary ReadConfigFile(string execPath)
        {
            var configMap = new StringOrderedDictionary();
            var execDir = Path.GetDirectoryName(execPath);
            var initialConfigFilename = Path.GetFileNameWithoutExtension(execPath) + ".cfg";
            var defaultConfigFilename = DefaultConfigFilename;
            var altConfigPath1 = Path.Combine(execDir, initialConfigFilename);
            var altConfigPath2 = Path.Combine(execDir, defaultConfigFilename);
            var configPaths = new List<string> { initialConfigFilename, defaultConfigFilename, altConfigPath1, altConfigPath2 };
            string configPath = configPaths.FirstOrDefault(File.Exists);
            if (configPath == null)
            {
                log("Failed to open any configuration file after trying config file locations:");
                foreach (var path in configPaths)
                {
                    log("  - \"" + Path.GetFullPath(path) + "\"");
                }
                return configMap;
            }
            using (var configFile = new StreamReader(configPath))
            {
                string line;
                while ((line = configFile.ReadLine()) != null)
                {
                    if (!string.IsNullOrWhiteSpace(line) && line[0] != '#')
                    {
                        string[] parts = line.Split('=');
                        if (parts.Length == 2)
                        {
                            string key = parts[0].Trim();
                            string value = parts[1].Trim().Trim('\'', '\"');
                            configMap[key] = value;
                        }
                    }
                }
            }
            return configMap;
        } // ReadConfigFile()
        
        public static StringKeyDictionary ReadAndParseConfigFile(string execPath)
        {
            var configStringMap = ReadConfigFile(execPath); 
            var configMap = new StringKeyDictionary();
            foreach (DictionaryEntry de in configStringMap)
            {
                bool isNumber = int.TryParse((string)de.Value, out int intVal);
                if (isNumber)
                {
                    configMap.Add(de.Key, intVal);
                    continue;
                }
                bool isBoolean = bool.TryParse((string)de.Value, out bool boolVal);
                if(isBoolean) {
                    configMap.Add(de.Key, boolVal);
                    continue;
                }
                configMap.Add(de.Key, (string)de.Value);
            }
            return configMap;
        } // ReadAndParseConfigFile()

        public static void CopyDirectory(string sourceDir, string destDir, bool updateFiles)
        {
            if (!Directory.Exists(destDir))
            {
                Directory.CreateDirectory(destDir);
            }
            foreach (string filePath in Directory.GetFiles(sourceDir))
            {
                string fileName = Path.GetFileName(filePath);
                string destPath = Path.Combine(destDir, fileName);
                File.Copy(filePath, destPath, updateFiles);
            }
            foreach (string subDir in Directory.GetDirectories(sourceDir))
            {
                string dirName = new DirectoryInfo(subDir).Name;
                string destSubDir = Path.Combine(destDir, dirName);
                CopyDirectory(subDir, destSubDir, updateFiles);
            }
        } // CopyDirectory()

        public static void RecursiveRemoveFiles(string sourceDir, bool removeFiles = false, bool removeDirectories = false, bool preserveSld = true, StreamWriter logger = null)
        {
            var sourceDirectory = new DirectoryInfo(sourceDir);
            /* Use default logFile if logger not provided */
            logger ??= logFile;
            
            /* (1): Before any removal actions, log what will generally be done */
            if (removeFiles)
            {
                if (preserveSld)
                {
                    log($"Removing all non-SLD files but no subdirectories from: {sourceDir}", logger);
                }
                else if (removeDirectories)
                {
                    log($"Removing all files and directories from: {sourceDir}", logger);
                }
                else
                {
                    log($"Removing only files from: ${sourceDir}", logger);
                }
            }
            else
            {
                log($"Not removing any files or subdirectories from: {sourceDir}", logger);
            }
            
            /* (2): Perform removal actions according to provided parameters */
            if (removeFiles)
            {
                var sourceDirectoryPath = sourceDirectory.FullName;
                var filesToRemove = Directory.GetFiles(sourceDirectoryPath, "*", SearchOption.AllDirectories);
                if (preserveSld)
                {
                    filesToRemove = filesToRemove.Where(file => !file.EndsWith(".sld", StringComparison.OrdinalIgnoreCase)).ToArray();
                }
                
                foreach (var filePath in filesToRemove)
                {
                    var file = new FileInfo(filePath);
                    logdbg($"Removing file: {file.FullName}", logger);
                    file.Delete();
                    logdbg($"Removed file: {file.FullName}", logger);
                }

                /* preserveSld overrides removing directories since we need to preserve the SLD files */
                if (removeDirectories && !preserveSld)
                {
                    foreach (var dir in sourceDirectory.GetDirectories("*", SearchOption.AllDirectories))
                    {
                        logdbg($"Removing directory: {dir.FullName}", logger);
                        dir.Delete();
                        logdbg($"Removed directory: {dir.FullName}", logger);
                    }
                    logdbg($"Removing base directory: {sourceDirectory.FullName}", logger);
                    sourceDirectory.Delete();
                    logdbg($"Removed base directory: {sourceDirectory.FullName}", logger);
                }
            }
        } // RecursiveRemoveFiles()

        private static string CreateSLDTempDirectory()
        {
            var tempDir = Directory.CreateTempSubdirectory("sld-copy-");
            var tempDirPath = tempDir.FullName;
            tempDirectories.Add(tempDirPath);
            log($"Temp directory created: '{tempDirPath}'");
            return tempDirPath;
        } // CreateSLDTempDirectory()

        private static void DeleteTempDirectories()
        {
            var tempDir = Path.GetTempPath();
            foreach (var tempDirPath in tempDirectories)
            {
                // Verify this is really a temp directory before recursively deleting it
                if (ContainsCaseInsensitiveSubstring(tempDirPath, tempDir) && Path.Exists(tempDirPath))
                {
                    log($"Deleting temp directory: '{tempDirPath}'");
                    Directory.Delete(tempDirPath, true);
                }
            }
        } // CreateSLDTempDirectory()

        private static ISequenceFileAccess ReadContentsOfSLDFile(string sldFilePath)
        {
            string sldFileCopy = sldFilePath;
            var sldFileInfo = new FileInfo(sldFilePath);
            if (!Path.Exists(sldFilePath))
            {
                var errorMessage = $"SLD file could not be read, file not found: '{sldFilePath}'";
                logerr(errorMessage);
                ShowErrorNotification("MassSpecTrigger Error", errorMessage);
                Environment.Exit(1);
            }
            var sldFileName = sldFileInfo.Name;
            var sldFileSize = sldFileInfo.Length;
            // Check for a current temporary copy of the SLD file
            if (tempDirectories is not null && tempDirectories.Count > 0)
            {
                foreach (var tempDir in tempDirectories)
                {
                    var tempDirFiles = Directory.EnumerateFiles(tempDir, "*", SearchOption.TopDirectoryOnly)
                        .Select(fp => new FileInfo(fp))
                        .Where(fi => fi.Name == sldFileName && fi.Length == sldFileSize).ToList();
                    if (tempDirFiles.Count == 1)
                    {
                        log($"Found existing temp copy of '{sldFilePath}', reading from copy");
                        sldFileCopy = tempDirFiles.First().FullName;
                        break;
                    }
                }
            }
            else
            {
                // Creates a copy of the SLD file in a temp directory before reading it
                var sldTempDir = CreateSLDTempDirectory();
                sldFileCopy = Path.Combine(sldTempDir, sldFileName);
                log($"Making temp copy of SLD file: '{sldFilePath}' => '{sldFileCopy}'");
                File.Copy(sldFilePath, sldFileCopy);
            }
            if (Path.Exists(sldFileCopy))
            {
                log($"Reading SLD file copy: '{sldFileCopy}'");
                var sldFile = SequenceFileReaderFactory.ReadFile(sldFileCopy);
                if (sldFile is null || sldFile.IsError)
                {
                    logerr($"Error opening the SLD file copy: {sldFileCopy}, {sldFile?.FileError.ErrorMessage}");
                    return sldFile;
                }
                if (!(sldFile is ISequenceFileAccess))
                {
                    logerr($"SLD file was read but contents are invalid: '{sldFileCopy}'");
                    return null;
                }

                return sldFile;
            }
            else
            {
                log($"Could not create temporary SLD file: '{sldFileCopy}'");
                return null;
            }
        } // ReadContentsOfSLDFile()

        // Just return a list of the RAW files in an SLD file. If fileNamesOnly, just return base names, not paths.
        // returns an empty list on errors
        private static List<string> GetSequenceFromSLD(string sldFilePath, bool fileNamesOnly = false)
        {
            List<string> rawFilePaths = new List<string>();
            // Initialize the SLD file reader
            var sldFile = ReadContentsOfSLDFile(sldFilePath);
            if (sldFile is null)
            {
                string errorMessage = $"Could not retrieve sequence from SLD file: '{sldFilePath}'";
                logerr(errorMessage);
                return null;
            }
            foreach (var sample in sldFile.Samples)
            {
                // I saw some blank-named .raw files in a FreeStyle SLD file and will skip these.
                if (string.IsNullOrEmpty(sample.RawFileName))
                {
                    continue;
                }
                // Sometimes a path sep and sometimes not
                // string rawFileName = sample.Path.TrimEnd('\\').ToLower() + Path.DirectorySeparatorChar + sample.RawFileName.ToLower() + ".raw";
                string rawFileName = sample.RawFileName + ".raw";
                string rawFilePath = Path.Combine(sample.Path.TrimEnd('\\'), rawFileName);
                string result = fileNamesOnly ? rawFileName : rawFilePath;
                rawFilePaths.Add(result);
            }
            // According to docs, the SLD file is not kept open., saw no dispose
            return rawFilePaths;
        }  // GetSequenceFromSLD()

        // SLD / Keep track of acquired  logic starts here
        // returns an empty dictionary on errors
        private static StringOrderedDictionary SLDReadSamples(string sldFilePath)
        {
            StringOrderedDictionary rawFilesAcquired = new StringOrderedDictionary();
            // Initialize the SLD file reader
            // var sldFile = SequenceFileReaderFactory.ReadFile(sldFilePath);

            // Read a copy of the SLD file
            var sldFile = ReadContentsOfSLDFile(sldFilePath);
            if (sldFile is null || sldFile.IsError)
            {
                logerr($"Error opening the SLD file: {sldFilePath}, {sldFile?.FileError.ErrorMessage}");
                return rawFilesAcquired;
            }
            foreach (var sample in sldFile.Samples)
            {
                // I saw some blank-named .raw files in a FreeStyle SLD file and will skip these.
                if (string.IsNullOrEmpty(sample.RawFileName))
                {
                    continue;
                }
                // Sometimes a path sep and sometimes not
                // string rawFileName = sample.Path.TrimEnd('\\').ToLower() + Path.DirectorySeparatorChar + sample.RawFileName.ToLower() + ".raw";
                // string rawFileName = Path.Combine(sample.Path.TrimEnd('\\').ToLower() + Path.DirectorySeparatorChar + sample.RawFileName.ToLower() + ".raw";
                string rawFileName = sample.RawFileName.ToLower() + ".raw";

                /* If we are ignoring PostBlank files, check that this RAW file is not a PostBlank */
                if (!(IgnorePostBlank && ContainsCaseInsensitiveSubstring(rawFileName, PostBlankMatches)))
                {
                    rawFilesAcquired[rawFileName] = "no";  // init all to unacquired
                }
            }
            // According to docs, the SLD file is not kept open., saw no dispose
            return rawFilesAcquired;
        }  // SLDReadSamples()

        private static StringOrderedDictionary MockSLDReadSamples(string rawFilesAcquiredPath, List<string> rawFilePaths)
        {
            StringOrderedDictionary rawFilesAcquired = readRawFilesAcquired(rawFilesAcquiredPath);
            foreach (var filePath in rawFilePaths)
            {
                if (string.IsNullOrEmpty(filePath))
                {
                    continue;
                }

                string rawFileName = Path.GetFileName(filePath).ToLower();

                /* If we are ignoring PostBlank files, check that this RAW file is not a PostBlank */
                if (!(IgnorePostBlank && ContainsCaseInsensitiveSubstring(rawFileName, PostBlankMatches)))
                {
                    rawFilesAcquired[rawFileName] = "no"; // init all to unacquired
                }
            }
            return rawFilesAcquired;
        }

        private static bool UpdateRawFilesAcquiredDict(string rawFilePath, StringOrderedDictionary rawFilesAcquired)
        {
            var rawFileName = Path.GetFileName(rawFilePath);
            if (IgnorePostBlank && ContainsCaseInsensitiveSubstring(rawFileName, PostBlankMatches))
            {
                log($"\"{rawFileName}\" appears to be a PostBlank file and will be ignored");
                return true;
            }
            if (rawFilesAcquired.Contains(rawFileName.ToLower()))
            {
                logdbg($"Acquisition status file has record for RAW file, setting acquired status to yes");
                rawFilesAcquired[rawFileName.ToLower()] = "yes";
                return true;
            }
            logerr($"Acquisition status file error: RAW file not found in SLD file: {rawFileName} error: RAW file not found in SLD file {SLD_FILE_PATH}");
            return false;
        }  // UpdateRawFilesAcquiredDict()

        private static bool areAllRawFilesAcquired(StringOrderedDictionary rawFilesAcquired)
        {
            logdbg($"{RAW_FILES_ACQUIRED_BASE} contents:\n{StringifyDictionary(rawFilesAcquired)}");
            foreach (DictionaryEntry entry in rawFilesAcquired)
            {
                if (string.Equals(entry.Value?.ToString(), "no", StringComparison.InvariantCulture))
                {
                    return false;
                }
            }
            log($"All raw files have been acquired.");
            return true;
        }

        // Assume a stripped internal file only
        private static void writeRawFilesAcquired(string rawFilesAcquiredPath, StringOrderedDictionary rawFilesAcquired)
        {
            using (StreamWriter writer = new StreamWriter(rawFilesAcquiredPath))
            {
                foreach (DictionaryEntry entry in rawFilesAcquired)
                {
                    string line = $"{entry.Key}={entry.Value}";
                    writer.WriteLine(line);
                }
            }
        } // writeRawFilesAcquired()

        // Assume a stripped internal file only
        private static StringOrderedDictionary readRawFilesAcquired(string rawFilesAcquiredPath)
        {
            StringOrderedDictionary rawFilesAcquired = new StringOrderedDictionary();
            if (File.Exists(rawFilesAcquiredPath))
            {
                logdbg($"Acquisition status file exists, reading values from: {rawFilesAcquiredPath}");
                // Read the file and populate the OrderedDictionary
                using (StreamReader reader = new StreamReader(rawFilesAcquiredPath))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] parts = line.Split('=');
                        if (parts.Length == 2)
                        {
                            string key = parts[0];
                            string value = parts[1];
                            rawFilesAcquired.Add(key.ToLower(), value.ToLower());
                        }
                    }
                }
            }
            else
            {
                logdbg($"Acquisition status file not found, will be initialized as empty: {rawFilesAcquiredPath}");
            }
            return rawFilesAcquired;
        }  // readRawFilesAcquired()

        private static int countRawFilesAcquired(string rawFilesAcquiredPath)
        {
            StringOrderedDictionary rawFilesAcquired = readRawFilesAcquired(rawFilesAcquiredPath);
            int acquired = 0;
            foreach (var value in rawFilesAcquired.Values)
            {
                var status = value.ToString();
                if (status == "yes")
                {
                    acquired++;
                }
            }
            return acquired;
        }  // countRawFilesAcquired()

        public static bool CheckForFailureFile(string destinationPath)
        {
            string msaFilePath = Path.Combine(destinationPath, FailureTokenFile);
            if (Path.Exists(destinationPath))
            {
                if (Path.Exists(msaFilePath))
                {
                    logdbg($"Failure trigger file already exists: '{msaFilePath}'");
                    return true;
                }
                else
                {
                    logdbg($"Failure trigger file does not already exist: '{msaFilePath}'");
                    return false;
                }
            }
            else
            {
                logerr($"Could not check for failure trigger file, directory does not exist: '{destinationPath}'");
                return false;
            }
        } // CheckForFailureFile()

        public static bool DeleteFailureFile(string destinationPath)
        {
            // Returns true if the failure trigger file is deleted or does not exist
            string msaFilePath = Path.Combine(destinationPath, FailureTokenFile);
            if (Path.Exists(destinationPath))
            {
                if (Path.Exists(msaFilePath))
                {
                    log($"Deleting failure trigger file: '{msaFilePath}'");
                    File.Delete(msaFilePath);
                } else {
                    log($"Did not delete failure trigger file, file not found: '{msaFilePath}'");
                }
            }
            else
            {
                log($"Error deleting failure trigger file, output directory not found: '{destinationPath}'");
                return false;
            }

            if (CheckForFailureFile(destinationPath))
            {
                logwarn($"Error, failure trigger file could not be deleted: '{msaFilePath}'");
                return false;
            }
            else
            {
                return true;
            }
        } // DeleteFailureFile()

        public static bool CreateTriggerFileSuccess(string destinationPath, string rawFilePath)
        {
            // write MSAComplete.txt to The Final Destination
            string ssDate = "trigger_date=\"" + TriggerFileTimestamp() + "\"";
            string ssRawFile = "raw_file=\"" + rawFilePath + "\"";
            string isRepeatRun = "false";
            if (ContainsCaseInsensitiveSubstring(destinationPath, RepeatRun) || ContainsCaseInsensitiveSubstring(rawFileName, RepeatRun))
            {
                isRepeatRun = "true";
            }
            string ssRepeat = "repeat_run=\"" + isRepeatRun + "\"";
            string msaFilePath = Path.Combine(destinationPath, TokenFile);
            using (StreamWriter msaFile = new StreamWriter(msaFilePath))
            {
                msaFile.WriteLine(ssDate);
                msaFile.WriteLine(ssRawFile);
                msaFile.WriteLine(ssRepeat);
            }
            
            // Check to see if failure trigger file exists. If so, delete it.
            if (CheckForFailureFile(destinationPath))
            {
                string msaFailurePath = Path.Combine(destinationPath, FailureTokenFile);
                if (DeleteFailureFile(destinationPath))
                {
                    log($"Successfully deleted old failure trigger file: '{msaFailurePath}'");
                }
                else
                {
                    logwarn($"Problem deleting old failure trigger file: '{msaFailurePath}'");
                }
            }
            log($"Trigger file created: '{msaFilePath}'");
            return true;
        } // CreateTriggerFileSuccess()

        public static bool CreateFailureTriggerFile(string destinationPath, string rawFilePath, string errorMessage)
        {
            log($"Got error '{errorMessage}', will create failure trigger file if required");

            // write MSAFailure.txt to The Final Destination
            string ssDate = "trigger_date=\"" + TriggerFileTimestamp() + "\"";
            string ssRawFile = "raw_file=\"" + rawFilePath + "\"";
            string isRepeatRun = "false";
            if (ContainsCaseInsensitiveSubstring(destinationPath, RepeatRun) || ContainsCaseInsensitiveSubstring(rawFileName, RepeatRun))
            {
                isRepeatRun = "true";
            }
            string ssRepeat = "repeat_run=\"" + isRepeatRun + "\"";
            CreateOutputDirectory(destinationPath);
            string msaFilePath = Path.Combine(destinationPath, FailureTokenFile);

            // Replace backslashes with forward slashes for compatibility with pipeline email function
            string safeError = Regex.Replace(errorMessage, @"\\", @"/"); 
            string ssError = "trigger_error=\"" + safeError + "\"";
            using (StreamWriter msaFile = new StreamWriter(msaFilePath))
            {
                msaFile.WriteLine(ssDate);
                msaFile.WriteLine(ssRawFile);
                msaFile.WriteLine(ssRepeat);
                msaFile.WriteLine(ssError);
            }
            log("Failure trigger file created: " + msaFilePath);
            return true;
        } // CreateTriggerFileFailure()

        public static string GetWindowsVersion()
        {
            var v = Environment.OSVersion.Version;
            return $"{v.Major}.{v.Minor}.{v.Build}";
        }

        public static void ShowErrorNotification(string title, string message)
        {
            try
            {
                var imgLogoName = @"error.png";
                // var imgLogoFullPath = Path.Combine(resourceFolderPath, imgLogoName);
                // var imgUri = new Uri("file://" + imgLogoFullPath);
                // var notice = new ToastContentBuilder()
                //     .AddText(title)
                //     .AddText(message);
                // if (File.Exists(imgLogoFullPath))
                // {
                //     logdbg($"Notification test: error image exists: '{imgUri}'");
                //     notice.AddAppLogoOverride(imgUri);
                // }
                // else
                // {
                //     logdbg($"Notification test: no error image found at: '{imgUri}'");
                // }
                //
                // notice.Show(toast =>
                // {
                //     toast.ExpirationTime = DateTime.Now.AddDays(1);
                // });
            }
            catch (Exception e)
            {
                log($"Error displaying notification: '{e.Message}'");
                log(e.StackTrace);
            }

        }

        public static void NotifyAboutError(string destinationPath, string rawFilePath, string errorMessage)
        {
            if (!CheckForFailureFile(destinationPath))
            {
                CreateFailureTriggerFile(destinationPath, rawFilePath, errorMessage);
                ShowErrorNotification("MassSpecTrigger Error", errorMessage);
            }
            else
            {
                string failureFile = Path.Combine(destinationPath, FailureTokenFile);
                log($"Failure trigger file already exists: '{failureFile}");
                log($"Not creating new failure trigger file for error: '{errorMessage}'");
            }
        }

        public static void Main(string[] args)
        {
            rawFileName = "";
            logFilePath = "";
            mockSequence = new List<string>();
            // StreamWriter logFile = null;
            parser = new CommandLine.Parser(with => with.HelpWriter = null);
            parserResult = parser.ParseArguments<Options>(args);
            parserResult
                .WithParsed<Options>(options => Run(options, args))
                .WithNotParsed<Options>(errs => DisplayHelp<Options>(parserResult, errs));
        } // Main()

        public static void DisplayHelp<T>(ParserResult<T> result, IEnumerable<Error> errs)
        {
            HelpText helpText = null;
            if (errs.IsVersion())
            {
                // helpText = HelpText.AutoBuild(result);
                DisplayVersion();
                Environment.Exit(0);
            }
            else
            {
                var usage = HelpText.RenderUsageText(result);
                helpText = HelpText.AutoBuild(result, h =>
                {
                    h.AdditionalNewLineAfterOption = true;
                    h.MaximumDisplayWidth = 120;
                    h.Heading = $"{AppName} v{AppVersion}".Pastel(Color.Cyan);
                    h.Copyright = "Copyright 2023 Mayo Clinic";
                    return HelpText.DefaultParsingErrorsHandler(result, h);
                }, e => e);
                Console.Error.WriteLine(usage);
                Console.Error.WriteLine();
                Console.Error.WriteLine(helpText);
            }
        } // DisplayHelp()

        public static void DisplayVersion()
        {
            var AppNameAndVersion = $"{AppName} v{AppVersion}".Pastel(Color.Cyan);
            var winVer = GetWindowsVersion();
            var SystemVersion = $"Windows {winVer}".Pastel(Color.Bisque);
            var buildTimestamp = GetBuildTimestamp();
            var ssBuildTime = $"Built: {buildTimestamp}".Pastel(Color.MediumSpringGreen);

            Console.Error.WriteLine(AppNameAndVersion);
            Console.Error.WriteLine(SystemVersion);
            Console.Error.WriteLine(ssBuildTime);
        }

        public static void Run(Options options, string[] args)
        {
            if (options.Debug)
            {
                DebugMode = true;
            }

            if (options.TestNotifications)
            {
                var curProc = System.Diagnostics.Process.GetCurrentProcess();
                string called = curProc.MainModule?.FileName;
                resourceFolderPath = Path.GetDirectoryName(called);
                var title = "MassSpecTrigger Testing";
                var text = "Testing MassSpecTrigger notifications\nAnd a second line for the MassSpecTrigger notification";
                ShowErrorNotification(title, text);
                Environment.Exit(0);
            }

            if (!string.IsNullOrEmpty(options.Logfile))
            {
                logFilePath = options.Logfile;
            }

            if (string.IsNullOrEmpty(options.InputRawFile))
            {
                var errMessage = "Please pass in the full path to a RAW file (using %R parameter in Xcalibur)";
                logerr(errMessage);
                ShowErrorNotification("MassSpecTrigger Error", errMessage);
                Environment.Exit(1);
            }
            else
            {
                rawFileName = options.InputRawFile;
            }

            if (!string.IsNullOrEmpty(options.MockSequence))
            {
                var str = options.MockSequence;
                var splits = str.Split(";");
                mockSequence.AddRange(splits.Select(filepath => filepath.Trim()).Where(contents => !string.IsNullOrEmpty(contents)));
                MockSequenceMode = true;
            }

            try
            {
                // BEG SETUP

                // Get the name of the current program (executable)
                var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
                string logPath = currentProcess.MainModule?.FileName;
                logFolderPath = Path.GetDirectoryName(logPath);
                resourceFolderPath = logFolderPath;
                if (string.IsNullOrEmpty(logFilePath))
                {
                    logFilePath = Path.Combine(logFolderPath, TriggerLogFileStem + "." + TriggerLogFileExtension);
                }
                logFile = new StreamWriter(logFilePath, true);
                logFile.AutoFlush = true;

                var exePath = currentProcess.MainModule?.FileName;
                log("");
                log("");
                log($"##############################################################");
                log($"# COMMAND: {exePath} {string.Join(" ", args)}");
                log($"##############################################################");
                log("");
                string rawFilePath = Path.GetFullPath(rawFileName);
                string rawFileBaseName = Path.GetFileName(rawFilePath);
                if (!File.Exists(rawFilePath))
                {
                    var errorMessage = $"{rawFileName} error: RAW file does not exist. Exiting.";
                    logerr(errorMessage);
                    ShowErrorNotification("MassSpecTrigger Error", errorMessage);
                    Environment.Exit(1);
                }
                string folderPath = Path.GetDirectoryName(rawFilePath);
                ConfigMap = ReadAndParseConfigFile(exePath);
                RepeatRun = ConfigMap.TryGetValue(RepeatRunKey, out string repeatRun) ? repeatRun : DefaultRepeatRun;
                TokenFile = ConfigMap.TryGetValue(TokenFileKey, out string tokenFile) ? tokenFile : DefaultTokenFile;
                FailureTokenFile = ConfigMap.TryGetValue(FailureTokenFileKey, out string failureTokenFile) ? failureTokenFile : DefaultFailureTokenFile;
                SourceTrim = ConfigMap.TryGetValue(SourceTrimKey, out string sourceTrimPath) ? sourceTrimPath : DefaultSourceTrim;
                SldStartsWith = ConfigMap.TryGetValue(SldStartsWithKey, out string sldStartsWith) ? sldStartsWith : DefaultSldStartsWith;
                IgnorePostBlank = ConfigMap.GetValueOrDefault(IgnorePostBlankKey, DefaultIgnorePostBlank);
                PostBlankMatches = ConfigMap.TryGetValue(PostBlankMatchesKey, out string postBlankMatches) ? postBlankMatches : DefaultPostBlankMatches;
                RemoveFiles = ConfigMap.GetValueOrDefault(RemoveFilesKey, DefaultRemoveFiles);
                RemoveDirectories = ConfigMap.GetValueOrDefault(RemoveDirectoriesKey, DefaultRemoveDirectories);
                PreserveSld = ConfigMap.GetValueOrDefault(PreserveSldKey, DefaultPreserveSld);
                UpdateFiles = ConfigMap.GetValueOrDefault(UpdateFilesKey, DefaultUpdateFiles);
                bool tempDebug = ConfigMap.GetValueOrDefault(DebugKey, DefaultDebugging);
                DebugMode = DebugMode ? DebugMode : tempDebug;
                MinRawFileSize = ConfigMap.GetValueOrDefault(MinRawFileSizeKey, DefaultMinRawFileSize);
                if (!ConfigMap.TryGetValue(OutputDirKey, out string outputPath))
                {
                    var errorMessage = "Missing key \"" + OutputDirKey + "\" in MassSpecTrigger configuration file. Exiting.";
                    logerr(errorMessage);
                    ShowErrorNotification("MassSpecTrigger Error", errorMessage);
                    Environment.Exit(1);
                }

                if (string.IsNullOrEmpty(SldStartsWith))
                {
                    logwarn($"Config key \"{SldStartsWithKey}\" not set, using default value: \"{DefaultSldStartsWith}\"");
                    SldStartsWith = DefaultSldStartsWith;
                }

                if (IgnorePostBlank)
                {
                    log($"PostBlank files will be ignored for this sequence");
                    log($"Any RAW file in this sequence with \"{PostBlankMatches}\" in its name will be ignored");
                    logdbg($"POSTBLANK: PostBlankMatches string is \"{PostBlankMatches}\" and raw file name is {rawFileBaseName}");
                    if (ContainsCaseInsensitiveSubstring(rawFileBaseName, PostBlankMatches))
                    {
                        log($"POSTBLANK: Provided RAW file '{rawFileBaseName}' is a PostBlank and will be ignored. Exiting.");
                        Environment.Exit(0);
                    }
                }
                else
                {
                    logwarn($"PostBlank files will not be ignored. This will cause an error if the PostBlank file is saved in a different directory");
                }
                // END SETUP
                
                // Determine final output path
                string destinationPath = ConstructDestinationPath(folderPath, outputPath, SourceTrim);

                // BEG SLD / Check acquired raw
                string sldPath = folderPath;
                string searchPattern = SldStartsWith + "*";
                string sldExtension = "sld";
                string sldFile = "";
                string rawFilesAcquiredPath = Path.Combine(sldPath, RAW_FILES_ACQUIRED_BASE);

                // All items in the dictionary are kept in lower case to avoid dealing with case sensitive files and strings.
                StringOrderedDictionary rawFilesAcquiredDict;

                // We only need to read the SLD file once; after that, we use the
                // acquisition status file. So let's see if it already exists.
                logdbg($"Checking for Acquisition status file: '{rawFilesAcquiredPath}'");
                if (File.Exists(rawFilesAcquiredPath))
                {
                    // Acquisition status file exists, no need to access SLD file
                    logdbg($"Acquisition status file exists at: '{rawFilesAcquiredPath}'");
                    rawFilesAcquiredDict = readRawFilesAcquired(rawFilesAcquiredPath);
                }
                else
                {
                    // Acquisition status file does not exist, so this is the first time
                    // we've been called for this run directory. We need to determine the
                    // list of RAW files in this sequence.
                    logdbg($"Acquisition status file does not exist: '{rawFilesAcquiredPath}'");
                    logdbg($"Looking for SLD file ...");
                    
                    // If we're in MockSequenceMode, use the provided list of RAW files instead of
                    // trying to find an SLD file.
                    if (MockSequenceMode)
                    {
                        log($"MOCK SEQUENCE MODE: mock sequence file contents are: [ {string.Join(", ", mockSequence)} ]");
                        if (!File.Exists(rawFilesAcquiredPath))
                        {
                            logdbg($"Acquisition status file does not exist, creating: '{rawFilesAcquiredPath}'");
                            rawFilesAcquiredDict = MockSLDReadSamples(rawFilesAcquiredPath, mockSequence);
                        }
                        else
                        {
                            logdbg($"Acquisition status file exists at: '{rawFilesAcquiredPath}'");
                            rawFilesAcquiredDict = readRawFilesAcquired(rawFilesAcquiredPath);
                        }

                        sldFile = Path.Combine(sldPath, "MOCK_SLD_FILE.sld");
                        SLD_FILE_PATH = sldFile;
                        logdbg($"Acquisition status file: '{rawFilesAcquiredPath}'");
                        logdbg($"Acquisition status file contents:\n{StringifyDictionary(rawFilesAcquiredDict)}'");
                    }
                    else
                    {
                        // No acquisition status file and we're not running in MockSequenceMode.
                        // Sigh. Now we need to find a good SLD file to use.
                        string[] sld_files;
                        logdbg($"SLD FILES: path is {sldPath}, search pattern is {searchPattern}, and SLD extension is {sldExtension}");
                        var all_files = Directory.GetFiles(sldPath, "*", SearchOption.TopDirectoryOnly);
                        // var allFiles
                        logdbg($"SLD FILES: directory \"{sldPath}\" contains files: [ {String.Join("; ", all_files)} ]");
                        // ReSharper disable once InconsistentNaming
                        var all_sld_files = all_files.Where(file => file.EndsWith(sldExtension, StringComparison.OrdinalIgnoreCase));
                        var allSldFiles = all_sld_files.ToList();
                        logdbg($"SLD FILES: files that end with '{sldExtension}': [ {String.Join("; ", allSldFiles)} ]");
                        var good_sld_files = allSldFiles.Where(file => !IsTempSldFile(file) && Path.GetFileName(file).StartsWith(SldStartsWith)).ToList();
                        sld_files = good_sld_files.ToArray();
                        logdbg($"SLD FILES: directory \"{sldPath}\" contains {sld_files.Length} non-temp SLD files: [ {String.Join("; ", sld_files)} ]");
                        if (sld_files.Length == 0)
                        {
                            // There are no SLD files. Nothing we can do, just exit.
                            var errorMessage = $"No SLD files found in directory: '{sldPath}'";
                            logerr(errorMessage);
                            NotifyAboutError(destinationPath, rawFilePath, errorMessage);
                            Environment.Exit(1);
                        }
                        else if (sld_files.Length > 1)
                        {
                            // There are multiple SLD files. We will try to figure out the right one.
                            // We will select the most recent SLD file with the correct name pattern.
                            logwarn($"Problem finding SLD file: directory '{sldPath}' contains {sld_files.Length} matching SLD files ({SldStartsWith}*.sld), directory should contain a single matching SLD file.");
                            var sld_files_info = good_sld_files.Select(fp => new FileInfo(fp)).ToList();
                            var sorted_sld_files = sld_files_info.OrderBy(f => f.LastWriteTime).ToList();
                            FileInfo sld_file_to_use = null;
                            if (sorted_sld_files.Count > 0)
                            {
                                sld_file_to_use = sorted_sld_files.Last();
                            }

                            if (sld_file_to_use is not null)
                            {
                                var sld_file_to_use_path = sld_file_to_use.FullName;
                                List<string> sldRawFileNames = GetSequenceFromSLD(sld_file_to_use_path, true);
                                List<string> sldRawFileLowercaseNames = sldRawFileNames.Select(f => f.ToLower()).ToList();
                                if (sldRawFileLowercaseNames.Contains(rawFileBaseName.ToLower()))
                                {
                                    // We will try this SLD file.
                                    logwarn($"Newest SLD file contains this RAW file's name, we will try using it: \"{sld_file_to_use_path}\"");
                                    sldFile = sld_file_to_use_path;
                                }
                                else
                                {
                                    string errorMessage = $"Newest SLD file does not contain this RAW file's name, cannot find SLD file to use, please remove extra SLD files from: '{folderPath}'";
                                    logerr(errorMessage);
                                    NotifyAboutError(destinationPath, rawFileName, errorMessage);
                                    Environment.Exit(1);
                                }
                            }
                            else
                            {
                                string errorMessage = $"Could not find required SLD file or use newest SLD file, please check that one SLD file exists in directory: '{folderPath}'";
                                logerr(errorMessage);
                                NotifyAboutError(destinationPath, rawFileName, errorMessage);
                                Environment.Exit(1);
                            }
                        }
                        else
                        {
                            // Exactly one SLD file exists in the directory. Things are good!
                            sldFile = sld_files[0];
                        }

                        log($"Using SLD file: {sldFile}.");
                        SLD_FILE_PATH = sldFile;
                        rawFilesAcquiredDict = SLDReadSamples(sldFile);
                    }
                }
                logdbg($"Acquisition status file: '{rawFilesAcquiredPath}'");
                if (rawFilesAcquiredDict.Count == 0)
                {
                    string errorMessage = $"Acquisition status internal dictionary is empty. Check {sldFile} and {rawFilesAcquiredPath}";
                    logerr(errorMessage);
                    NotifyAboutError(destinationPath, rawFileName, errorMessage);
                    Environment.Exit(1);
                }

                if (!UpdateRawFilesAcquiredDict(rawFileName, rawFilesAcquiredDict))
                {
                    string errorMessage = $"Could not update acquisition status file '{rawFilesAcquiredPath}' for sample '{rawFileName}'";
                    logerr(errorMessage);
                    NotifyAboutError(destinationPath, rawFileName, errorMessage);
                    Environment.Exit(1);
                }
                log($"Updated acquisition status for RAW file {rawFileName}");
                writeRawFilesAcquired(rawFilesAcquiredPath, rawFilesAcquiredDict);
                // END SLD / Check acquired raw

                int total = rawFilesAcquiredDict.Count;
                int acquired = countRawFilesAcquired(rawFilesAcquiredPath);
                
                // BEG WRITE MSA
                // BEG MOVE FOLDER
                if (areAllRawFilesAcquired(rawFilesAcquiredDict))
                {
                    log($"{acquired}/{total} raw files acquired, beginning payload activity ...");
                    if (!PrepareOutputDirectory(destinationPath, MinRawFileSize, RawFilePattern))
                    {
                        var errorMessage = "Could not prepare destination: \"" + destinationPath + "\". Check this directory. Exiting.";
                        logerr(errorMessage);
                        ShowErrorNotification("MassSpecTrigger Error", errorMessage);
                        Environment.Exit(1);
                    }
                    log("Copying directory: \"" + folderPath + "\" => \"" + destinationPath + "\"");
                    CopyDirectory(folderPath, destinationPath, UpdateFiles);
                    RecursiveRemoveFiles(folderPath, RemoveFiles, RemoveDirectories, PreserveSld, logFile);
                    // if (RemoveFiles)
                    // {
                    //     
                    //     log("Removing source directory: \"" + folderPath + "\"");
                    //     Directory.Delete(folderPath, true);
                    // }
                    // else
                    // {
                    //     log("Config value '" + RemoveFilesKey + "' is not true, not deleting source directory");
                    // }
                    // END MOVE FOLDER
                    
                    // write MSAComplete.txt to The Final Destination
                    if (CreateTriggerFileSuccess(destinationPath, rawFileName))
                    {
                        log("Processing completed successfully");
                        Environment.Exit(0);
                    }
                    else
                    {
                        string msaFile = Path.Combine(destinationPath, TokenFile);
                        string errorMessage = $"Could not save trigger file '{msaFile}'";
                        logerr(errorMessage);
                        NotifyAboutError(destinationPath, rawFileName, errorMessage);
                        Environment.Exit(1);
                    }
                }
                else
                {
                    log($"{acquired}/{total} raw files acquired, not performing payload activities yet");
                }
                // END WRITE MSA
            }  // try
            catch (Exception ex)
            {
                logerr("Error: " + ex.Message);
            }
            finally
            {
                logFile?.Close();
            }

        } // Run()

    }  // MainClass()
}  // ns MassSpecTrigger

