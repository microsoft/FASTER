using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    internal struct DprFinderCommand
    {
        internal enum Type
        {
            NEW_CHECKPOINT,
            REPORT_RECOVERY,
            SYNC
        }

        internal Type commandType;
        internal WorkerVersion wv;
        internal long worldLine;
        internal List<WorkerVersion> deps;
    }

    internal enum RedisMessageType
    {
        SIMPLE_STRING,
        ARRAY,
        ERROR,
        BULK_STRING
    }

    internal enum ParserState
    {
        NONE,
        NUM_ARGS,
        COMMAND_TYPE,
        ARG_WV,
        ARG_WL,
        ARG_DEPS,
    }
    
    internal class SimpleRedisParser
    {
        internal DprFinderCommand currentCommand;
        internal ParserState parserState;
        internal int currentCommandStart = -1, currentFragmentStart, size, stringStart;

        internal SimpleRedisParser()
        {
            currentCommand.deps = new List<WorkerVersion>();
        }

        private void ProcessCommandStart(int readHead, byte[] buf)
        {
            currentCommandStart = readHead;
            // Initialize to an invalid 
            size = -1;
            switch ((char) buf[readHead])
            {
                case '*':
                    parserState = ParserState.NUM_ARGS;
                    currentFragmentStart = readHead;
                    break;
                default:
                    throw new NotImplementedException("Unsupported RESP syntax --- we only" +
                                                      "support DPR commands sent as BULK_STRING");
            }
        }

        private bool ProcessRedisInt(int readHead, byte[] buf, out long result)
        {
            result = default;
            if (buf[readHead - 1] != '\r' || buf[readHead] != '\n') return false;
            result = MessageUtil.LongFromDecimalString(buf, currentFragmentStart + 1, readHead - 1);
            // Fragment has ended
            currentFragmentStart = readHead + 1;
            return true;
        }

        private bool ProcessRedisBulkString(int readHead, byte[] buf)
        {
            // account for \r\n in the end of string field
            if (readHead == stringStart + size + 2)
            {
                // Fragment has ended
                currentFragmentStart = readHead + 1;
                return true;
            }

            if (size == -1 && buf[readHead] == '\n' && buf[readHead - 1] == '\r')
            {
                // This is the first field, should read the size. The integer size field starts one past
                // the message type byte and ends at '\r'
                size = (int) MessageUtil.LongFromDecimalString(buf, currentFragmentStart + 1, readHead - 1);

                if (size == -1) throw new NotImplementedException("Null Bulk String not supported");

                stringStart = readHead + 1;
            }

            return false;
        }

        public unsafe bool ProcessChar(int readHead, byte[] buf)
        {
            switch (parserState)
            {
                case ParserState.NONE:
                    ProcessCommandStart(readHead, buf);
                    return false;
                case ParserState.NUM_ARGS:
                {
                    if (ProcessRedisInt(readHead, buf, out var size))
                    {
                        Debug.Assert(size == 1 || size == 3);
                        parserState = ParserState.COMMAND_TYPE;
                    }

                    return false;
                }
                case ParserState.COMMAND_TYPE:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        if (buf[stringStart] == 'N')
                        {
                            Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                .Equals("NewCheckpoint"));
                            currentCommand.commandType = DprFinderCommand.Type.NEW_CHECKPOINT;
                            parserState = ParserState.ARG_WV;
                        }
                        else if (buf[stringStart] == 'R')
                        {
                            Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                .Equals("ReportRecovery"));
                            currentCommand.commandType = DprFinderCommand.Type.REPORT_RECOVERY;
                            parserState = ParserState.ARG_WV;
                        }
                        else if (buf[stringStart] == 'S')
                        {
                            Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size).Equals("SYNC"));
                            currentCommand.commandType = DprFinderCommand.Type.SYNC;
                            parserState = ParserState.NONE;
                            return true;
                        }
                    }

                    return false;
                case ParserState.ARG_WV:
                    // TODO(Tianyu): change WorkerVersion to 8 bytes.
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        Debug.Assert(size == sizeof(WorkerVersion));
                        // TODO(Tianyu): Call WorkerVersion relevant methods
                        var workerId = BitConverter.ToInt32(buf, stringStart);
                        var version = BitConverter.ToInt32(buf, stringStart + sizeof(int));
                        currentCommand.wv = new WorkerVersion(workerId, version);
                        if (currentCommand.commandType == DprFinderCommand.Type.NEW_CHECKPOINT)
                        {
                            parserState = ParserState.ARG_DEPS;
                        }
                        else if (currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY)
                        {
                            parserState = ParserState.ARG_WL;
                        }
                        else
                        {
                            Debug.Assert(false);
                        }
                    }
                    return false;
                case ParserState.ARG_WL:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        Debug.Assert(size == sizeof(long));
                        Debug.Assert(currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY);
                        currentCommand.worldLine = BitConverter.ToInt32(buf, stringStart);
                        parserState = ParserState.NONE;
                        return true;
                    }
                    return false;
                case ParserState.ARG_DEPS:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        Debug.Assert(currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY);
                        currentCommand.deps.Clear();
                        var numDeps = BitConverter.ToInt32(buf, stringStart);
                        for (var i = 0; i < numDeps; i++)
                        {
                            // TODO(Tianyu): Replace with WV version
                            var workerId = BitConverter.ToInt32(buf, stringStart + sizeof(int) + i * sizeof(WorkerVersion));
                            var version = BitConverter.ToInt32(buf, stringStart + 2 * sizeof(int) + i * sizeof(WorkerVersion));
                            currentCommand.deps.Add(new WorkerVersion(workerId, version));
                        }
                        parserState = ParserState.NONE;
                        return true;
                    }
                    return false;
                default:
                    throw new NotImplementedException("Unrecognized Parser state");
            }
        }
    }
}