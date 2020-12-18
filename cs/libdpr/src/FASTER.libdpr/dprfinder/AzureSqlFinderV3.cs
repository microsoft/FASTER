using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace FASTER.libdpr
{
    public class AzureSqlDprFinderV3 : IDprFinder
    {
        private readonly Worker me;
        private Dictionary<Worker, long> recoverableCut;
        private long systemWorldLine;
        private readonly SqlConnection writeConn, readConn;
        private Socket dprFinderConn;
        private byte[] buffer = new byte[1 << 15];

        public AzureSqlDprFinderV3(string connString, Worker me)
        {
            this.me = me;
            recoverableCut = new Dictionary<Worker, long>();
            writeConn = new SqlConnection(connString);
            readConn = new SqlConnection(connString);
            writeConn.Open();
            readConn.Open();
            var registration = new SqlCommand($"EXEC upsertVersion @worker={me.guid}, @version=0, @oegVersion=0",
                writeConn);
            registration.ExecuteNonQuery();
            var worldLines = new SqlCommand($"INSERT INTO worldLines VALUES({me.guid}, 0)", writeConn);
            worldLines.ExecuteNonQuery();

            var ip = IPAddress.Parse("10.0.1.7");
            var endPoint = new IPEndPoint(ip, 15000);
            var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            sender.NoDelay = true;
            sender.Connect(endPoint);
        }

        public long SafeVersion(Worker worker)
        {
            return !recoverableCut.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            return new V3DprTableSnapshot(recoverableCut);
        }

        public long SystemWorldLine()
        {
            return systemWorldLine;
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // V1 does not use the safeVersion column, can always use 0
            var upsert = new SqlCommand($"EXEC reportRecoveryV3 @workerId={latestRecoveredVersion.Worker.guid}," +
                                        $"@worldLine={worldLine}, @survivingVersion={latestRecoveredVersion.Version}",
                writeConn);
            upsert.ExecuteNonQuery();
        }

        public long GlobalMaxVersion()
        {
            // Only used for fast-forwarding of versions. Not required for v3.
            return 0;
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            var depsList = deps.ToList();
            var stream = new MemoryStream(buffer);
            var writer = new BinaryWriter(stream);
            var size = 16 + 4 + 16 * depsList.Count;
            writer.Write(size);
            writer.Write(persisted.Worker.guid);
            writer.Write(persisted.Version);
            writer.Write(depsList.Count);
            foreach (var wv in depsList)
            {
                writer.Write(wv.Worker.guid);
                writer.Write(wv.Version);
            }

            dprFinderConn.Send(buffer);
        }

        public void Refresh()
        {
            var newRecoverableCut = new Dictionary<Worker, long>(recoverableCut);
            var selectCommand = new SqlCommand($"EXEC getTableUpdatesV3", readConn);
            var reader = selectCommand.ExecuteReader();
            var hasNextRow = reader.Read();
            Debug.Assert(hasNextRow);
            systemWorldLine = (long) reader[1];

            var hasNextResultSet = reader.NextResult();
            Debug.Assert(hasNextResultSet);
            while (reader.Read())
            {
                var worker = new Worker(long.Parse((string) reader[0]));
                newRecoverableCut[worker] = (long) reader[1];
            }

            // Has to be after the update to global min, so any races are benign
            recoverableCut = newRecoverableCut;
            reader.Close();
        }

        public void Clear()
        {
            writeConn.Dispose();
            readConn.Dispose();
            dprFinderConn.Close();
        }
    }
}