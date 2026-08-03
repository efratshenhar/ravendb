using System;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBiLicenseClaimTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const string Claimed = "ordinary SQL claimed as a Power BI query - requires a Power BI license";

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        [InlineData(@"SELECT * FROM public.""Orders""")]
        [InlineData(@"SELECT ""Company"", ""Freight"" FROM public.""Orders""")]
        [InlineData(@"SELECT ""Company"" FROM public.""Orders"" WHERE ""Freight"" > 1")]
        [InlineData(@"SELECT ""Company"" FROM public.""Orders"" ORDER BY ""Company"" LIMIT 10")]
        [InlineData(@"SELECT DISTINCT ""Company"" FROM public.""Orders""")]
        public void Ordinary_public_qualified_sql_is_not_a_power_bi_query(string sql)
        {
            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _), $"{Claimed}: {sql}");
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        [InlineData(@"SELECT * FROM ""Orders""")]
        [InlineData(@"SELECT ""Company"", ""Freight"" FROM ""Orders""")]
        [InlineData(@"SELECT ""Company"" FROM ""Orders"" WHERE ""Freight"" > 1")]
        [InlineData(@"SELECT ""Company"" FROM ""Orders"" ORDER BY ""Company"" LIMIT 10")]
        [InlineData(@"SELECT DISTINCT ""Company"" FROM ""Orders""")]
        public void Ordinary_unqualified_sql_is_not_a_power_bi_query(string sql)
        {
            Assert.False(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _), $"{Claimed}: {sql}");
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        [InlineData(@"select ""_"".""Company"", ""_"".""Freight"" from ""public"".""Orders"" ""_"" limit 1000")]
        [InlineData(@"select * from ""public"".""Orders"" ""$Table""")]
        public void Generated_power_bi_sql_is_a_power_bi_query(string sql)
        {
            Assert.True(PowerBIQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out _), $"genuine Power BI query not claimed: {sql}");
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
        [InlineData(@"SELECT * FROM ""Orders""", @"SELECT * FROM public.""Orders""")]
        [InlineData(@"SELECT ""Company"", ""Freight"" FROM ""Orders""", @"SELECT ""Company"", ""Freight"" FROM public.""Orders""")]
        [InlineData(@"SELECT ""Company"" FROM ""Orders"" WHERE ""Freight"" > 1", @"SELECT ""Company"" FROM public.""Orders"" WHERE ""Freight"" > 1")]
        [InlineData(@"SELECT ""Company"" FROM ""Orders"" ORDER BY ""Company"" LIMIT 10", @"SELECT ""Company"" FROM public.""Orders"" ORDER BY ""Company"" LIMIT 10")]
        [InlineData(@"SELECT DISTINCT ""Company"" FROM ""Orders""", @"SELECT DISTINCT ""Company"" FROM public.""Orders""")]
        public void Public_schema_prefix_must_not_change_the_power_bi_claim(string unqualified, string publicQualified)
        {
            var unqualifiedClaimed = PowerBIQuery.TryParse(unqualified, Array.Empty<int>(), documentDatabase: null, out _);
            var publicQualifiedClaimed = PowerBIQuery.TryParse(publicQualified, Array.Empty<int>(), documentDatabase: null, out _);

            Assert.True(unqualifiedClaimed == publicQualifiedClaimed,
                $"the public. prefix alone flipped the Power BI claim ({unqualifiedClaimed} -> {publicQualifiedClaimed}) - {Claimed}: {publicQualified}");
        }
    }
}
