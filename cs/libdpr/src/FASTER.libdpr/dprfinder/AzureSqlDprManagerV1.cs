using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    public class AzureSqlDprManagerV1: IDprManager
    {
        // cached local value
        private readonly Worker me;
        private long globalSafeVersionNum = 0, globalMaxVersionNum = 1, systemWorldLine = 0;
        private readonly SqlConnection writeConn, readConn;

        public AzureSqlDprManagerV1(string connString, Worker me, bool register = true)
        {
            this.me = me;
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

        public void Clear()
        {
            writeConn.Dispose();
            readConn.Dispose();
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // V1 does not use the safeVersion column, can always use 0
            var upsert = new SqlCommand($"EXEC reportRecoveryV1 @workerId={latestRecoveredVersion.Worker.guid}," +
                                        $"@worldLine={worldLine}, @survivingVersion={latestRecoveredVersion.Version}", writeConn);
            upsert.ExecuteNonQuery();
        }

        public long SafeVersion(Worker worker)
        {
            return globalSafeVersionNum;
        }

        public long GlobalMaxVersion()
        {
            return globalMaxVersionNum;
        }

        public long SystemWorldLine()
        {
            return systemWorldLine;
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            return new V1DprTableSnapshot(globalSafeVersionNum);
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            // V1 does not use the safeVersion column, can always use 0
            var upsert = new SqlCommand($"EXEC upsertVersion @worker={persisted.Worker.guid}," +
                                        $"@version={persisted.Version}, @oegVersion=0", writeConn);
            upsert.ExecuteNonQuery();
        }

        public void Refresh()
        {
            var selectCommand = new SqlCommand("EXEC getTableUpdatesV1", readConn);
            var reader = selectCommand.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            var hasResults = reader.Read();
            Debug.Assert(hasResults);
            globalMaxVersionNum = (long) reader[0];
            globalSafeVersionNum = (long) reader[1];
            systemWorldLine = (long) reader[2];
            reader.Close();
        }
    }
}