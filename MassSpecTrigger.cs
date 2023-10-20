// See https://aka.ms/new-console-template for more information
using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using System.Xml;

/*
 * Test data in subfolder test.
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank2.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank3.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank4.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank5.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank6.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank7.raw",
"D:\\Amyloid_Standards\\2023\\AmyloidStandard10_20231010_Ex1_PostBlanks_NewTrigger\\AmyloidStandard10_20231010_Ex1_27min_PostBlank8.raw"

"C:\\Xcalibur\\Data\\SWG_serum_100512053549.raw",
"C:\\Xcalibur\\Data\\SWG_serum_100512094915.raw"

"D:\\Data\\GFB\\matt\\08112011\\PBS_IP_IKBalpha_SMTA.raw"
*/

namespace MassSpecTrigger
{
    // to avoid most casting
    public class StringOrderedDictionary : OrderedDictionary
    {
        public /*new*/ string this[string key]
        {
            get
            {
                return (string)base[key];
            }
            set
            {
                base[key] = value;
            }
        }
    }  // StringOrderedDictionary

    class MainClass
    {
        private const string RAW_FILES_ACQUIRED_BASE = "RawFilesAcquired.txt";

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Please provide the complete path a RAW file.");
                return;
            }
            string raw_file = args[0];
            // check for raw file existence from previous C++ program
            string raw_file_dir = Directory.GetParent(raw_file).FullName;
            string sldPath = raw_file_dir;
            string[] sld_files = Directory.GetFiles(sldPath, "*.sld", SearchOption.TopDirectoryOnly)
                .Where(file => file.EndsWith(".sld", StringComparison.OrdinalIgnoreCase))
                .ToArray();
            if (sld_files.Length != 1)
            {
                Console.WriteLine($"Check {sldPath} for a single SLD file.");
                Environment.Exit(1);
            }
            string sldFile = sld_files[0];
            Console.WriteLine($"Initial work based on SLD file: Check {sldFile}.");

            string rawFilesAcquiredPath = sldPath + Path.DirectorySeparatorChar + RAW_FILES_ACQUIRED_BASE;
            // All items in the dictionary are kept ion lower case to avoid dealing with case sensitive files and strings.
            StringOrderedDictionary rawFilesAcquiredDict = new StringOrderedDictionary();
            // only need to read the SLD the first time in dir.
            if (!File.Exists(rawFilesAcquiredPath))
            {
                rawFilesAcquiredDict = SLDReadSamples(sldFile);
            }
            else
            {
                rawFilesAcquiredDict = readRawFilesAcquired(rawFilesAcquiredPath);
            }
            if (rawFilesAcquiredDict.Count == 0)
            {
                Console.WriteLine("$The raw files acquired internal dictionary is empty. Check {sldFile} and {rawFilesAcquiredPath}");
                Environment.Exit(1);
            }

            if (!UpdateRawFilesAcquiredDict(raw_file, rawFilesAcquiredDict))
            {
                Environment.Exit(1);
            }
            Console.WriteLine($"Updated {raw_file} for acquisition state.");
            writeRawFilesAcquired(rawFilesAcquiredPath, rawFilesAcquiredDict);
            if (areAllRawFilesAcquired(rawFilesAcquiredDict))
            {
                // write MSAComplete.txt
            }

        } // Main()

        // returns an empty dictionary on errors
        private static StringOrderedDictionary SLDReadSamples(string sldFilePath)
        {
            StringOrderedDictionary rawFilesAcquired = new StringOrderedDictionary();
            // Initialize the SLD file reader
            var sldFile = SequenceFileReaderFactory.ReadFile(sldFilePath);
            if (sldFile.IsError)
            {
                Console.WriteLine($"Error opening the SLD file: {sldFilePath}, {sldFile.FileError.ErrorMessage}");
                return rawFilesAcquired;
            }
            if (!(sldFile is ISequenceFileAccess))
            {
                Console.WriteLine($"This file {sldFilePath} does not support sequence file access.");
                return rawFilesAcquired;
            }
            foreach (var sample in sldFile.Samples)
            {
                // I saw some blamk-named .raw files in a FreeStyle SLD file and will skip these.
                if (string.IsNullOrEmpty(sample.RawFileName))
                {
                    continue;
                }
                // Sometimes a path sep and sometimes not
                string rawFileName = sample.Path.TrimEnd('\\').ToLower() + Path.DirectorySeparatorChar
                    + sample.RawFileName.ToLower() + ".raw";
                rawFilesAcquired[rawFileName] = "no";  // init all to unacquired
            }
            // According to docs, the SLD file is not kept open., saw no dispose
            return rawFilesAcquired;
        }  // SLDReadSamples()

        private static bool UpdateRawFilesAcquiredDict(string rawFilePath, StringOrderedDictionary rawFilesAcquired)
        {
            if (rawFilesAcquired.Contains(rawFilePath.ToLower()))
            {
                rawFilesAcquired[rawFilePath.ToLower()] = "yes";
                return true;
            }
            else
            {
                Console.WriteLine($"{rawFilePath} is not in SLD file, please check triggered raw file and SLD file.");
                return false;
            }
        }  // UpdateRawFilesAcquiredDict()

        private static bool areAllRawFilesAcquired(StringOrderedDictionary rawFilesAcquired)
        {
            foreach (DictionaryEntry entry in rawFilesAcquired)
            {
                if (string.Equals(entry.Value.ToString(), "no", StringComparison.InvariantCulture))
                {
                    return false;
                }
            }
            Console.WriteLine($"All raw files have been acquired.");
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
            return rawFilesAcquired;
        }  // readRawFilesAcquired()

    }  // MainClass()
}  // ns MassSpecTrigger

