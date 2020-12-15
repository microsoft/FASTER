using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace FASTER.libdpr
{
    public class AzureSqlOwnershipMapping : IOwnershipMapping
    {

        private SemaphoreSlim latch;
        private readonly SqlConnection conn;

        public AzureSqlOwnershipMapping(string connString)
        {
            latch = new SemaphoreSlim(1, 1);
            conn = new SqlConnection(connString);
            conn.Open();
        }
        
        public async ValueTask<Worker> LookupAsync(long bucket)
        {
            using var _ = await latch.LockAsync();
            using var lookup = new SqlCommand("EXEC findOwner @bucket=@B", conn);
            var b = new SqlParameter("@B", SqlDbType.VarChar) { Value = bucket };
            lookup.Parameters.Add(b);
            return new Worker((long) await lookup.ExecuteScalarAsync());
        }

        public async ValueTask<Worker> ObtainOwnershipAsync(long bucket, Worker newOwner, Worker expectedOwner)
        {
            using var _ = await latch.LockAsync();
            using var lookup = new SqlCommand("EXEC obtainOwnership @bucket=@B, @newOwner=@N, @expectedOwner=@e", conn);
            var b = new SqlParameter("@B", SqlDbType.VarChar) { Value = bucket };
            var n = new SqlParameter("@N", SqlDbType.BigInt) { Value = newOwner.guid };
            var e = new SqlParameter("@E", SqlDbType.BigInt) { Value = expectedOwner.guid };

            lookup.Parameters.Add(b);
            lookup.Parameters.Add(n);
            lookup.Parameters.Add(e);

            var reader = await lookup.ExecuteReaderAsync();
            await reader.ReadAsync();
            var owner = new Worker((long) reader[0]);
            reader.Close();
            return owner;        
        }

        public async ValueTask RemoveAsync(long bucket)
        {
            using var _ = await latch.LockAsync();
            using var delete = new SqlCommand("EXEC remove @bucket=@B", conn);
            var b = new SqlParameter("@B", SqlDbType.VarChar) {
                Value = bucket
            };
            delete.Parameters.Add(b);
            await delete.ExecuteNonQueryAsync();
        }
    }
}