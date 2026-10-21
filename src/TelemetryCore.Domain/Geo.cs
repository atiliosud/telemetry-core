namespace TelemetryCore.Domain;

public static class Geo
{
    private const double EarthRadiusKm = 6371.0088;

    public static double HaversineKm(double lat1, double lon1, double lat2, double lon2)
    {
        const double toRad = Math.PI / 180.0;
        var dLat = (lat2 - lat1) * toRad;
        var dLon = (lon2 - lon1) * toRad;
        var sinLat = Math.Sin(dLat / 2);
        var sinLon = Math.Sin(dLon / 2);
        var a = sinLat * sinLat + Math.Cos(lat1 * toRad) * Math.Cos(lat2 * toRad) * sinLon * sinLon;
        return 2 * EarthRadiusKm * Math.Asin(Math.Min(1.0, Math.Sqrt(a)));
    }
}
