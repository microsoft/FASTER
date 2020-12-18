﻿﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    public class AzureSqlDprFinderV2 : IDprFinder
    {
        private readonly Worker me;
        private Dictionary<Worker, long> recoverableCut;
        private long globalSafeVersionNum = 0, globalMaxVersionNum = 1, systemWorldLine = 0;
        private readonly SqlConnection writeConn, readConn;

        private readonly List<long> outstandingVersions;
        private readonly List<IEnumerable<WorkerVersion>> outstandingVersionsDeps;

        public AzureSqlDprFinderV2(string connString, Worker me, bool register = true)
        {
            this.me = me;
            recoverableCut = new Dictionary<Worker, long>();
            outstandingVersions = new List<long>();
            outstandingVersionsDeps = new List<IEnumerable<WorkerVersion>>();
            writeConn = new SqlConnection(connString);
            readConn = new SqlConnection(connString);
            writeConn.Open();
            readConn.Open();

            if (register)
            {
                var registration = new SqlCommand($"EXEC upsertVersion @worker={me.guid}, @version=0, @oegVersion=0",
                    writeConn);
                registration.ExecuteNonQuery();
                var worldLines = new SqlCommand($"INSERT INTO worldLines VALUES({me.guid}, 0)", writeConn);
                worldLines.ExecuteNonQuery();
            }
        }
        
        public long SafeVersion(Worker worker)
        {
            if (!recoverableCut.TryGetValue(worker, out var tableValue))
                tableValue = 0;
            return Math.Max(tableValue, globalSafeVersionNum);
        }

        public long SystemWorldLine()
        {
            return systemWorldLine;
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            // These might not be atomic, but so long as the globalSafeVerisonNum value is newed than the recoverable
            // cut, the end result is still correct. We ensure this when refreshing the local CPR table view.
            // It is also safe to just capture a reference to the recoverable cut without protection, because the
            // update process will atomic swap in a new one instead of updating it in-place.
            return new V2DprTableSnapshot(globalSafeVersionNum, recoverableCut);
        }

        public long GlobalMaxVersion()
        {
            return globalMaxVersionNum;
        }
        
        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // V1 does not use the safeVersion column, can always use 0
            var upsert = new SqlCommand($"EXEC reportRecoveryV1 @workerId={latestRecoveredVersion.Worker.guid}," +
                                        $"@worldLine={worldLine}, @survivingVersion={latestRecoveredVersion.Version}", writeConn);
            upsert.ExecuteNonQuery();
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            outstandingVersions.Add(persisted.Version);
            outstandingVersionsDeps.Add(deps);
            var readSnapshot = ReadSnapshot();
            var oegVersion = readSnapshot.SafeVersion(me);
            for (var i = outstandingVersions.Count - 1; i >= 0; i--)
            {
                if (!outstandingVersionsDeps[i]
                    .All(dep => readSnapshot.SafeVersion(dep.Worker) >= dep.Version)) continue;
                oegVersion = outstandingVersions[i];
                outstandingVersions.RemoveRange(0, i + 1);
                outstandingVersionsDeps.RemoveRange(0, i + 1);
                break;
            }
            
            var upsert = new SqlCommand($"EXEC upsertVersion @worker={persisted.Worker.guid}," +
                                        $"@version={persisted.Version}, @oegVersion={oegVersion}", writeConn);
            upsert.ExecuteNonQuery();
        }

        public void Refresh()
        {
            var newRecoverableCut = new Dictionary<Worker, long>(recoverableCut);
            var selectCommand = new SqlCommand("EXEC getTableUpdatesV2", readConn);
            var reader = selectCommand.ExecuteReader();
            while (reader.Read())
            {
                var worker = new Worker(long.Parse((string) reader[0]));
                newRecoverableCut[worker] = (long) reader[1];
            }

            var hasNextResultSet = reader.NextResult();
            Debug.Assert(hasNextResultSet);
            var hasNextRow = reader.Read();
            Debug.Assert(hasNextRow);
            globalMaxVersionNum = (long) reader[0];
            globalSafeVersionNum = (long) reader[1];
            systemWorldLine = (long) reader[2];
            // Has to be after the update to global min, so any races are benign
            recoverableCut = newRecoverableCut;
            reader.Close();
        }

        public void Clear()
        {
            writeConn.Dispose();
            readConn.Dispose();        
        }
    }
}