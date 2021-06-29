using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    internal class DprFinderResponseParser
    {
        internal int size = -1;
        internal int stringStart = -1;

        public bool ProcessChar(int readHead, byte[] buf)
        {
            if (readHead == 0)
            {
                Debug.Assert((char) buf[readHead] == '$');
                size = -1;
            }

            switch ((char) buf[readHead])
            {
                case '\n':
                    if (buf[readHead - 1] != '\r') return false;
                    if (size == -1)
                    {
                        // Implicit message start at 0 always
                        size = (int) RespUtil.LongFromDecimalString(buf, 1, readHead - 1);
                        stringStart = readHead + 1;
                        return false;
                    }

                    return readHead == stringStart + size + 1;
                default:
                    // Nothing to do
                    return false;
            }
        }
    }

    internal struct DprFinderCommand
    {
        internal enum Type
        {
            NEW_CHECKPOINT,
            REPORT_RECOVERY,
            ADD_WORKER,
            DELETE_WORKER,
            SYNC
        }

        internal Type commandType;
        internal WorkerVersion wv;
        internal Worker w;
        internal long worldLine;
        internal List<WorkerVersion> deps;
    }

    internal enum CommandParserState
    {
        NONE,
        NUM_ARGS,
        COMMAND_TYPE,
        ARG_W,
        ARG_WV,
        ARG_WL,
        ARG_DEPS,
    }

    internal class DprFinderCommandParser
    {
        internal DprFinderCommand currentCommand;
        internal CommandParserState commandParserState;
        internal int currentCommandStart = -1, currentFragmentStart, size, stringStart;

        internal DprFinderCommandParser()
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
                    commandParserState = CommandParserState.NUM_ARGS;
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
            result = RespUtil.LongFromDecimalString(buf, currentFragmentStart + 1, readHead - 1);
            // Fragment has ended
            currentFragmentStart = readHead + 1;
            return true;
        }

        private bool ProcessRedisBulkString(int readHead, byte[] buf)
        {
            // account for \r\n in the end of string field
            if (size != -1 && readHead == stringStart + size + 1)

            {
                // Fragment has ended
                currentFragmentStart = readHead + 1;
                return true;
            }

            if (size == -1 && buf[readHead] == '\n' && buf[readHead - 1] == '\r')
            {
                // This is the first field, should read the size. The integer size field starts one past
                // the message type byte and ends at '\r'
                size = (int) RespUtil.LongFromDecimalString(buf, currentFragmentStart + 1, readHead - 1);

                if (size == -1) throw new NotImplementedException("Null Bulk String not supported");

                stringStart = readHead + 1;
            }

            return false;
        }

        internal unsafe bool ProcessChar(int readHead, byte[] buf)
        {
            switch (commandParserState)
            {
                case CommandParserState.NONE:
                    ProcessCommandStart(readHead, buf);
                    return false;
                case CommandParserState.NUM_ARGS:
                {
                    if (ProcessRedisInt(readHead, buf, out var size))
                        commandParserState = CommandParserState.COMMAND_TYPE;
                    return false;
                }
                case CommandParserState.COMMAND_TYPE:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        switch ((char) buf[stringStart])
                        {
                            case 'N':
                                Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                    .Equals("NewCheckpoint"));
                                currentCommand.commandType = DprFinderCommand.Type.NEW_CHECKPOINT;
                                commandParserState = CommandParserState.ARG_WV;
                                break;
                            case 'R':
                                Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                    .Equals("ReportRecovery"));
                                currentCommand.commandType = DprFinderCommand.Type.REPORT_RECOVERY;
                                commandParserState = CommandParserState.ARG_WV;
                                break;
                            case 'A':
                                Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                    .Equals("AddWorker"));
                                currentCommand.commandType = DprFinderCommand.Type.ADD_WORKER;
                                commandParserState = CommandParserState.ARG_W;
                                break;
                            case 'D':
                                Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size)
                                    .Equals("DeleteWorker"));
                                currentCommand.commandType = DprFinderCommand.Type.DELETE_WORKER;
                                commandParserState = CommandParserState.ARG_W;
                                break;
                            case 'S':
                                Debug.Assert(System.Text.Encoding.ASCII.GetString(buf, readHead, size).Equals("Sync"));
                                currentCommand.commandType = DprFinderCommand.Type.SYNC;
                                commandParserState = CommandParserState.NONE;
                                return true;
                            default:
                                throw new NotImplementedException("Unrecognized command type");
                        }

                        size = -1;
                    }

                    return false;
                case CommandParserState.ARG_W:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        var workerId = BitConverter.ToInt64(buf, stringStart);
                        currentCommand.w = new Worker(workerId);
                        commandParserState = CommandParserState.NONE;
                        size = -1;
                        return true;
                    }

                    return false;
                case CommandParserState.ARG_WV:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        // TODO(Tianyu): Call WorkerVersion relevant methods instead of hard-coded deserialization
                        Debug.Assert(size == sizeof(WorkerVersion));
                        var workerId = BitConverter.ToInt64(buf, stringStart);
                        var version = BitConverter.ToInt64(buf, stringStart + sizeof(int));
                        currentCommand.wv = new WorkerVersion(workerId, version);
                        if (currentCommand.commandType == DprFinderCommand.Type.NEW_CHECKPOINT)
                        {
                            commandParserState = CommandParserState.ARG_DEPS;
                        }
                        else if (currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY)
                        {
                            commandParserState = CommandParserState.ARG_WL;
                        }
                        else
                        {
                            Debug.Assert(false);
                        }

                        size = -1;
                    }

                    return false;
                case CommandParserState.ARG_WL:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        Debug.Assert(size == sizeof(long));
                        Debug.Assert(currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY);
                        currentCommand.worldLine = BitConverter.ToInt64(buf, stringStart);
                        commandParserState = CommandParserState.NONE;
                        size = -1;
                        return true;
                    }

                    return false;
                case CommandParserState.ARG_DEPS:
                    if (ProcessRedisBulkString(readHead, buf))
                    {
                        Debug.Assert(currentCommand.commandType == DprFinderCommand.Type.REPORT_RECOVERY);
                        currentCommand.deps.Clear();
                        var numDeps = BitConverter.ToInt32(buf, stringStart);
                        for (var i = 0; i < numDeps; i++)
                        {
                            // TODO(Tianyu): Replace with WV version
                            var workerId = BitConverter.ToInt32(buf,
                                stringStart + sizeof(int) + i * sizeof(WorkerVersion));
                            var version = BitConverter.ToInt32(buf,
                                stringStart + 2 * sizeof(int) + i * sizeof(WorkerVersion));
                            currentCommand.deps.Add(new WorkerVersion(workerId, version));
                            size = -1;
                        }

                        commandParserState = CommandParserState.NONE;
                        return true;
                    }

                    return false;
                default:
                    throw new NotImplementedException("Unrecognized Parser state");
            }
        }
    }
}