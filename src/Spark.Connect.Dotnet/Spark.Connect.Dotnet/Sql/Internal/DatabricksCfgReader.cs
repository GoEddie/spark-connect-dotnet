namespace Spark.Connect.Dotnet.Sql;

public class DatabricksCfgReader
{
    public Dictionary<string, string> GetProfile(string profileName)
    {
        var values = new Dictionary<string, string>();
        var mode = Mode.SearchingForProfile;

        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var configPath = Path.Join(homeDir, ".databrickscfg");

        if (!File.Exists(configPath))
        {
            throw new ArgumentException(
                $"Cannot use profile when no profile file exists: '{new FileInfo(configPath).FullName}'");
        }

        foreach (var line in File.ReadAllLines(configPath))
        {
            if (mode == Mode.SearchingForProfile)
            {
                if (IsProfileNameLine(line))
                {
                    if ($"[{profileName}]" == line.Trim())
                    {
                        mode = Mode.InProfile;
                        continue;
                    }
                }
            }

            if (mode == Mode.InProfile)
            {
                if (IsProfileNameLine(line))
                {
                    return values;
                }

                var parts = line.Split('=');
                values.Add(parts[0].Trim(), parts[1].Trim());
            }
        }

        if (values.Count == 0)
        {
            throw new ArgumentException(
                $"Profile '{profileName}' was not found in '{new FileInfo(configPath).FullName}'");
        }

        return values;
    }

    private static bool IsProfileNameLine(string line)
    {
        return line.Trim().StartsWith('[') && line.Trim().EndsWith(']');
    }

    private enum Mode
    {
        SearchingForProfile,
        InProfile
    }
}