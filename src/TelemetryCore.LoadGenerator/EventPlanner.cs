using System.Text;
using System.Text.Json;
using TelemetryCore.Contracts;

namespace TelemetryCore.LoadGenerator;

/// <summary>
/// Plans exactly Total messages with exact fault counts (round(total x ratio)), positions chosen with the seeded RNG.
/// Out-of-order swaps take two consecutive valid events of one vehicle and emit them reversed.
/// </summary>
public sealed class EventPlanner
{
    private const int RingSize = 10_000;

    private readonly GeneratorOptions _o;
    private readonly TimeProvider _clock;
    private readonly long _started;
    private readonly long _speedingVehicles;
    private long _sent, _dups, _invalid, _ooo;

    public EventPlanner(GeneratorOptions o, TimeProvider clock)
    {
        _o = o;
        _clock = clock;
        _started = clock.GetTimestamp();
        _speedingVehicles = Math.Min(o.Vehicles, Exact(o.Vehicles, o.SpeedingVehiclesPercent));
    }

    public RunSummary Summary
    {
        get
        {
            var seconds = _clock.GetElapsedTime(_started).TotalSeconds;
            return new RunSummary(_sent, _dups, _invalid, _ooo, _sent - _dups - _invalid, _speedingVehicles,
                seconds, seconds > 0 ? _sent / seconds : 0);
        }
    }

    private static long Exact(long total, double percent) =>
        (long)Math.Round(total * percent / 100.0, MidpointRounding.AwayFromZero);

    public IEnumerable<PlannedMessage> Plan()
    {
        var total = _o.Total;
        var rng = new Random(_o.Seed);
        var nDup = Exact(total, _o.DuplicatePercent);
        var nInv = Exact(total, _o.InvalidPercent);
        var nSwap = Exact(total, _o.OutOfOrderPercent);
        var valid = total - nDup - nInv;
        if (nDup + nInv > total - 1 || valid < 1 || nSwap * 2 > valid)
            throw new ArgumentException("Fault ratios leave too few valid messages for the requested total.");

        // Slot kinds: slot 0 is always valid so duplicates always have something to repeat.
        var kinds = new byte[total]; // 0 valid, 1 duplicate, 2 invalid
        var slots = Enumerable.Range(1, (int)(total - 1)).ToArray();
        Shuffle(slots, rng);
        for (var i = 0; i < nDup; i++) kinds[slots[i]] = 1;
        for (var i = 0; i < nInv; i++) kinds[slots[nDup + i]] = 2;

        // Swap pairs occupy consecutive valid ordinals (2k, 2k+1).
        var pairs = Enumerable.Range(0, (int)(valid / 2)).ToArray();
        Shuffle(pairs, rng);
        var swapStart = new HashSet<long>();
        for (var i = 0; i < nSwap; i++) swapStart.Add(pairs[i] * 2L);

        var fleet = new FleetSimulator(_o.Vehicles, (int)_speedingVehicles, rng);
        var ring = new PlannedMessage[RingSize];
        var ringCount = 0;
        long ringNext = 0;
        long validOrdinal = 0;
        long invalidOrdinal = 0;
        PlannedMessage? held = null;

        for (long slot = 0; slot < total; slot++)
        {
            PlannedMessage msg;
            switch (kinds[slot])
            {
                case 1:
                    var src = ring[rng.Next(ringCount)];
                    msg = new PlannedMessage(src.Key, src.Payload, MessageKind.Duplicate);
                    _dups++;
                    break;
                case 2:
                    msg = BuildInvalid(fleet, invalidOrdinal++);
                    _invalid++;
                    break;
                default:
                    if (held is { } h)
                    {
                        msg = h;
                        held = null;
                    }
                    else if (swapStart.Contains(validOrdinal))
                    {
                        var idx = fleet.PickIndex();
                        var now = _clock.GetUtcNow();
                        var a = fleet.Next(idx, now);
                        var b = fleet.Next(idx, now);
                        var key = fleet.IdOf(idx);
                        held = new PlannedMessage(key, FleetSimulator.Serialize(a), MessageKind.Valid);
                        msg = new PlannedMessage(key, FleetSimulator.Serialize(b), MessageKind.Valid);
                        _ooo++;
                    }
                    else
                    {
                        var idx = fleet.PickIndex();
                        msg = new PlannedMessage(fleet.IdOf(idx), FleetSimulator.Serialize(fleet.Next(idx, _clock.GetUtcNow())), MessageKind.Valid);
                    }
                    validOrdinal++;
                    ring[ringNext % RingSize] = msg;
                    ringNext++;
                    if (ringCount < RingSize) ringCount++;
                    break;
            }

            _sent++;
            yield return msg;
        }
    }

    private PlannedMessage BuildInvalid(FleetSimulator fleet, long ordinal)
    {
        var idx = fleet.PickIndex();
        var key = fleet.IdOf(idx);
        var now = _clock.GetUtcNow();
        var dto = fleet.Scratch(idx, now);
        byte[] payload;
        switch (ordinal % 5)
        {
            case 0:
                dto.Lat = 95;
                payload = FleetSimulator.Serialize(dto);
                break;
            case 1:
                dto.SpeedKmh = 400;
                payload = FleetSimulator.Serialize(dto);
                break;
            case 2:
                dto.TimestampUtc = now - TimeSpan.FromDays(2);
                payload = FleetSimulator.Serialize(dto);
                break;
            case 3:
                var good = FleetSimulator.Serialize(dto);
                payload = good.AsSpan(0, good.Length / 2).ToArray(); // truncated JSON
                break;
            default:
                var json = Encoding.UTF8.GetString(FleetSimulator.Serialize(dto));
                payload = Encoding.UTF8.GetBytes(json[..^1] + ",\"pad\":\"" + new string('x', 5 * 1024) + "\"}");
                break;
        }
        return new PlannedMessage(key, payload, MessageKind.Invalid);
    }

    private static void Shuffle(int[] a, Random rng)
    {
        for (var i = a.Length - 1; i > 0; i--)
        {
            var j = rng.Next(i + 1);
            (a[i], a[j]) = (a[j], a[i]);
        }
    }
}
