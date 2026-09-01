namespace NServiceBus.Transport.SqlServer.UnitTests;

using NUnit.Framework;

public class SqlServerTransportPatchTests
{
    [Test]
    public void Describe_identifies_the_patch_and_fits_in_a_log_message()
    {
        var description = SqlServerTransportPatch.Describe();

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain(SqlServerTransportPatch.Version));
            Assert.That(description, Does.Contain("counters since"));
            Assert.That(description.Length, Is.LessThan(4000), "must fit the promised 4k budget");
        });
    }
}
