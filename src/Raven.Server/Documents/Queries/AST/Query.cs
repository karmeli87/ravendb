using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Exceptions;
using Sparrow;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public GraphQuery GraphQuery;
        public QueryExpression Where;
        public FromClause From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<(QueryExpression Expression, StringSegment? Alias)> GroupBy;

        public Dictionary<string, DeclaredFunction> DeclaredFunctions;

        public string QueryText;
        public (string FunctionText, Esprima.Ast.Program Program) SelectFunctionBody;
        public string UpdateBody;
        public ValueExpression Offset;
        public ValueExpression Limit;

        public bool TryAddFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            return DeclaredFunctions.TryAdd(func.Name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }

        public void TryAddWithClause(Query query, StringSegment alias, bool implicitAlias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();                
            }

            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var existing))
            {
                if (query.From.From.Compound.Count == 0)
                    return; // reusing an alias defined explicitly before

                if (existing.withQuery.From.From.Compound.Count == 0)
                {
                    // using an alias that is defined _later_ in the query
                    GraphQuery.WithDocumentQueries[alias] = (implicitAlias, query);
                    return;
                }

                throw new InvalidQueryException($"Alias {alias} is already in use on a different 'With' clause", QueryText);
            }

            GraphQuery.WithDocumentQueries.Add(alias, (implicitAlias, query));
        }

        public void TryAddWithEdgePredicates(WithEdgesExpression expr, StringSegment alias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();               
            }

            if (GraphQuery.WithEdgePredicates.ContainsKey(alias))
            {
                if (expr.Path.Compound.Count == 0 && expr.OrderBy == null && expr.Where == null)
                    return;

                throw new InvalidQueryException($"Allias {alias} is already in use on a diffrent 'With' clause",
                    QueryText, null);
            }

            GraphQuery.WithEdgePredicates.Add(alias, expr);
        }

        public bool TryAddTimeSeriesFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            func.Name = Constants.TimeSeries.QueryFunction + DeclaredFunctions.Count;

            return DeclaredFunctions.TryAdd(func.Name, func);
        }
    }

    public class DeclaredFunction
    {
        public string Name;
        public string FunctionText;
        public Esprima.Ast.Program JavaScript;
        public TimeSeriesFunction TimeSeries;
        public FunctionType Type;
        public List<QueryExpression> Parameters;

        public enum FunctionType
        {
            JavaScript,
            TimeSeries
        }
    }

    public unsafe class TimeSeriesFunction
    {
        public TimeSeriesBetweenExpression Between;
        public QueryExpression Where;
        public ValueExpression GroupBy;
        public List<(QueryExpression, StringSegment?)> Select;
        public StringSegment? LoadTagAs;
        public TimeSpan? Offset;

        public struct RangeGroup
        {
            public long Ticks;
            public int Months;

            public DateTime GetRangeStart(DateTime timestamp)
            {
                var ticks = timestamp.Ticks;

                if (Ticks != 0)
                {
                    ticks -= (ticks % Ticks);
                    return new DateTime(ticks,timestamp.Kind);
                }

                if (Months != 0)
                {
                    var yearsPortion = Math.Max(1, Months / 12);
                    var monthsRemaining = Months % 12;
                    var year = timestamp.Year - (timestamp.Year % yearsPortion);
                    int month = monthsRemaining == 0 ? 1 : ((timestamp.Month - 1) / monthsRemaining * monthsRemaining) + 1;

                    return new DateTime(year, month, 1, 0, 0, 0, timestamp.Kind);
                }
                return timestamp;
            }

            public DateTime GetNextRangeStart(DateTime timestamp)
            {
                if (Ticks != 0)
                {
                    return timestamp.AddTicks(Ticks);
                }

                if (Months != 0)
                {
                    return timestamp.AddMonths(Months);
                }
                return timestamp;
            }
        }


        public static RangeGroup ParseRangeFromString(string s)
        {
            var range = new RangeGroup();
            var offset = 0;

            var duration = ParseNumber(s, ref offset);
            ParseRange(s, ref offset, ref range, duration);

            while (offset < s.Length && char.IsWhiteSpace(s[offset]))
            {
                offset++;
            }
            if (offset != s.Length)
                throw new ArgumentException("After range specification, found additional unknown data: " + s);

            return range;
        }

        private static void ParseRange(string source, ref int offset, ref RangeGroup range, long duration)
        {
            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            while (char.IsWhiteSpace(source[offset]) && offset < source.Length)
            {
                offset++;
            }

            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            switch (char.ToLower(source[offset++]))
            {
                case 's':
                    if (TryConsumeMatch(source, ref offset, "seconds") == false)
                        TryConsumeMatch(source, ref offset, "second");

                    range.Ticks += duration * 10_000_000;
                    return;
                case 'm':
                    if (TryConsumeMatch(source, ref offset, "minutes") ||
                        TryConsumeMatch(source, ref offset, "minute") ||
                        TryConsumeMatch(source, ref offset, "min"))
                    {
                        range.Ticks += duration * 10_000_000 * 60;
                        return;
                    }

                    if (TryConsumeMatch(source, ref offset, "ms") ||
                        TryConsumeMatch(source, ref offset, "milli") ||
                        TryConsumeMatch(source, ref offset, "milliseconds"))
                    {
                        range.Ticks += duration;
                        return;
                    }
                    if (TryConsumeMatch(source, ref offset, "months") ||
                        TryConsumeMatch(source, ref offset, "month") ||
                        TryConsumeMatch(source, ref offset, "mon"))
                    {
                        AssertValidDurationInMonths(duration);
                        range.Months+= (int)duration;
                        return;
                    }
                    range.Ticks += duration * 10_000_000 * 60;
                    return;
                case 'h':
                    if (TryConsumeMatch(source, ref offset, "hours") == false)
                        TryConsumeMatch(source, ref offset, "hour");

                    range.Ticks += duration * 10_000_000 * 60 * 60;
                    return;
                case 'd':
                    if (TryConsumeMatch(source, ref offset, "days") == false)
                        TryConsumeMatch(source, ref offset, "day");
                    range.Ticks += duration * 10_000_000 * 60 * 60 * 24;
                    return;
                case 'q':
                    if (TryConsumeMatch(source, ref offset, "quarters") == false)
                        TryConsumeMatch(source, ref offset, "quarter");
                    duration *= 3;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;

                case 'y':
                    if (TryConsumeMatch(source, ref offset, "years") == false)
                        TryConsumeMatch(source, ref offset, "year");
                    duration *= 12;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;
                default:
                    throw new ArgumentException($"Unable to understand time range: '{source}'");
            }
        }

        private static void AssertValidDurationInMonths(long duration)
        {
            if (duration > 120_000)
                throw new ArgumentException("The specified range results in invalid range, cannot have: " + duration + " months");
        }

        private static bool TryConsumeMatch(string source, ref int offset, string additionalMatch)
        {
            if (source.Length <= offset)
                return false;

            if (new StringSegment(source, offset-1 , source.Length - offset +1).StartsWith(additionalMatch, StringComparison.OrdinalIgnoreCase))
            {
                offset += additionalMatch.Length-1;
                return true;
            }
            return false;
        }

        private static long ParseNumber(string source, ref int offset)
        {
            int i;
            for (i= offset; i < source.Length; i++)
            {
                if (char.IsWhiteSpace(source[i]) == false)
                    break;
            }

            for (; i < source.Length; i++)
            {
                if (char.IsNumber(source[i]) == false)
                    break;
            }

            fixed(char* s = source)
            {
                if (long.TryParse(new ReadOnlySpan<char>(s + offset, i), out var amount) )
                {
                    offset = i;
                    return amount;
                }
            }

            throw new ArgumentException("Unable to parse: '" + source.Substring(offset) + "' as a number");
        }
    }


    public struct TimeSeriesAggregation
    {
        public AggregationType Aggregation;
        public bool Any => _values.Count > 0;

        private readonly List<object> _values;
        private readonly List<object> _count;

        public IEnumerable<object> Count => _count;

        public TimeSeriesAggregation(AggregationType type)
        {
            Aggregation = type;
            _count = new List<object>();
            _values = new List<object>();
        }

        public void Init()
        {
            _count.Clear();
            _values.Clear();
        }

        public void Segment(Span<StatefulTimestampValue> values)
        {
            if (_count.Count < values.Length)
            {
                for (int i = _count.Count; i < values.Length; i++)
                {
                    _count.Add(0L);
                    _values.Add(0d);
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if ((long)_count[i] == 0)
                            _values[i] = val.Min;
                        else
                            _values[i] = Math.Min((double)_values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if ((long)_count[i] == 0)
                            _values[i] = val.Max;
                        else
                            _values[i] = Math.Max((double)_values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Avg:
                    case AggregationType.Mean:
                        _values[i] = (double)_values[i] + val.Sum;
                        break;
                    case AggregationType.First:
                        if ((long)_count[i] == 0)
                            _values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        _values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                _count[i] = (long)_count[i] + values[i].Count;
            }
        }

        public void Step(Span<double> values)
        {
            if (_count.Count < values.Length)
            {
                for (int i = _count.Count; i < values.Length; i++)
                {
                    _count.Add(0L);
                    _values.Add(0d);
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if ((long)_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Min((double)_values[i], val);
                        break;
                    case AggregationType.Max:
                        if ((long)_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Max((double)_values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Avg:
                    case AggregationType.Mean:
                        _values[i] = (double)_values[i] + val;
                        break;
                    case AggregationType.First:
                        if ((long)_count[i] == 0)
                            _values[i] = val;
                        break;
                    case AggregationType.Last:
                        _values[i] = val;
                        break;
                    case AggregationType.Count:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                _count[i] = (long)_count[i] + 1;
            }
        }

        public IEnumerable<object> GetFinalValues()
        {
            switch (Aggregation)
            {
                case AggregationType.Min:
                case AggregationType.Max:
                case AggregationType.First:
                case AggregationType.Last:
                case AggregationType.Sum:
                    break;
                case AggregationType.Count:
                    return Count;
                case AggregationType.Mean:
                case AggregationType.Avg:
                    for (int i = 0; i < _values.Count; i++)
                    {
                        if ((long)_count[i] == 0)
                        {
                            _values[i] = double.NaN;
                            continue;
                        }

                        _values[i] = (double)_values[i] / (long)_count[i];
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
            }

            return _values;
        }
    }

}
