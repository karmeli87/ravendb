﻿using System;
using System.Linq;
using System.Collections.Generic;
using Sparrow.Collections;
using System.Diagnostics;
using Sparrow.Binary;

namespace Sparrow.Json
{
    /// <summary>
    /// This class allows us to cache the properties from multiple documents
    /// That way, we don't need to recompute the sort order.
    /// 
    /// However, because different documents may have different fields, we're tracking
    /// which properties we have seen and which are new. If the order in which we read the 
    /// properties doesn't match the cached version, we'll clear the cache up to the point the
    /// properties match. 
    /// 
    /// This is done so we'll not write properties that don't belong to that document, but to 
    /// previous documents handled in the same batch
    /// </summary>
    public class CachedProperties : IComparer<BlittableJsonDocumentBuilder.PropertyTag>
    {
        private readonly JsonOperationContext _context;
        private int _propertyNameCounter;

        public sealed class PropertyName :IComparable<PropertyName>
        {
            public LazyStringValue Comparer;
            public int GlobalSortOrder;
            public int PropertyId;
            public readonly int HashCode;

            public PropertyName(int hash)
            {
                HashCode = hash;
            }

            public int CompareTo(PropertyName other)
            {
                return Comparer.CompareTo(other.Comparer);
            }

            public override string ToString()
            {
                return $"Value: {Comparer}, GlobalSortOrder: {GlobalSortOrder}, PropertyId: {PropertyId}";
            }

            private bool Equals(PropertyName other)
            {
                return HashCode == other.HashCode;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                var a = obj as PropertyName;
                return a != null && Equals(a);
            }

            public override int GetHashCode()
            {
                return HashCode;
            }
        }

        private class PropertyPosition
        {
            public PropertyName Property;
            public int SortedPosition;
            public BlittableJsonDocumentBuilder.PropertyTag Tmp;
        }

        private class CachedSort
        {
            public readonly List<PropertyPosition> Sorting = new List<PropertyPosition>();
            public int FinalCount;

            public override string ToString()
            {
                return string.Join(", ", Sorting.Select(x => x.Property.Comparer));
            }
        }

        private const int CachedSortsSize = 512;
        private readonly CachedSort[] _cachedSorts = new CachedSort[CachedSortsSize]; // size is fixed and used in GetPropertiesHashedIndex

        private readonly List<PropertyName> _docPropNames = new List<PropertyName>();
        private readonly SortedDictionary<PropertyName, object> _propertiesSortOrder = new SortedDictionary<PropertyName, object>();
        private readonly FastDictionary<LazyStringValue, PropertyName, LazyStringValueStructComparer> _propertyNameToId = new FastDictionary<LazyStringValue, PropertyName, LazyStringValueStructComparer>(default(LazyStringValueStructComparer));
        private bool _propertiesNeedSorting;

        public int PropertiesDiscovered;

        public CachedProperties(JsonOperationContext context)
        {
            _context = context;
        }

        public PropertyName GetProperty(LazyStringValue propName)
        {
            PropertyName prop;
            if (_propertyNameToId.TryGetValue(propName, out prop) == false)
            {
                var propIndex = _docPropNames.Count;
                propName = _context.GetLazyStringForFieldWithCaching(propName);
                prop = new PropertyName(_propertyNameCounter++)
                {
                    Comparer = propName,
                    GlobalSortOrder = -1,
                    PropertyId = propIndex
                };

                _docPropNames.Add(prop);
                _propertiesSortOrder.Add(prop, prop);
                _propertyNameToId[propName] = prop; 
                _propertiesNeedSorting = true;
                if (_docPropNames.Count > PropertiesDiscovered+1)
                {
                    prop = SwapPropertyIds(prop);
                }
                PropertiesDiscovered++;
            }
            else if (prop.PropertyId >= PropertiesDiscovered)
            {
                if (prop.PropertyId != PropertiesDiscovered)
                {
                    prop = SwapPropertyIds(prop);
                }
                PropertiesDiscovered++;
            }
            return prop;
        }

        private PropertyName SwapPropertyIds(PropertyName prop)
        {
            // this property doesn't match the order that we previously saw the properties.
            // it is possible that this is a completely new format, or just properties
            // in different order. 
            // we'll assume the later and move the property around, this is safe to 
            // do because we ignore the properties showing up after the PropertiesDiscovered

            var old = _docPropNames[PropertiesDiscovered];
            _docPropNames[PropertiesDiscovered] = _docPropNames[prop.PropertyId];
            old.PropertyId = _docPropNames[PropertiesDiscovered].PropertyId;
            _docPropNames[old.PropertyId] = old;
            prop = _docPropNames[PropertiesDiscovered];
            prop.PropertyId = PropertiesDiscovered;
            return prop;
        }

        public void Sort(FastList<BlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            var index = GetPropertiesHashedIndex(properties);

            // Sort object properties metadata by property names
            if (_propertiesNeedSorting)
            {
                UpdatePropertiesSortOrder();
            }

            var cachedSort = _cachedSorts[index];
            if (cachedSort?.Sorting.Count != properties.Count)
            {
                UnlikelySortProperties(properties);
                return;
            }

            // we are frequently going to see documents with ids in the same order
            // so we can take advantage of that by remember the previous sort, we 
            // check if the values are the same, and if so, save the sort

            for (int i = 0; i < properties.Count; i++)
            {
                if (cachedSort.Sorting[i].Property == properties[i].Property)
                {
                    cachedSort.Sorting[i].Tmp = properties[i];
                }
                else
                    {
                    UnlikelySortProperties(properties);
                    return;
                    }
                }

            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < cachedSort.Sorting.Count; i++)
            {
                properties[cachedSort.Sorting[i].SortedPosition] = cachedSort.Sorting[i].Tmp;
            }

            if (properties.Count != cachedSort.FinalCount)
            {
                properties.RemoveRange(cachedSort.FinalCount, properties.Count - cachedSort.FinalCount);
            }

            
        }

        private int GetPropertiesHashedIndex(FastList<BlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            int hash = 0;
            for (int i = 0; i < properties.Count; i++)
            {
                hash = (hash*397) ^ properties[i].Property.HashCode;
            }

            Debug.Assert(_cachedSorts.Length == CachedSortsSize &&
                         Bits.NextPowerOf2(CachedSortsSize) == CachedSortsSize); 

            hash &= (CachedSortsSize-1); // % CachedSortsSize 
            return hash;
        }

        private void UnlikelySortProperties(FastList<BlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            _hasDuplicates = false;

            var index = GetPropertiesHashedIndex(properties);

            if (_cachedSorts[index] == null)
                _cachedSorts[index] = new CachedSort();

            _cachedSorts[index].Sorting.Clear();

            for (int i = 0; i < properties.Count; i++)
            {
                _cachedSorts[index].Sorting.Add(new PropertyPosition
                {
                    Property = properties[i].Property,
                    SortedPosition = -1
                });
            }

            _cachedSorts[index].FinalCount = properties.Count;
            properties.Sort(this);

            // The item comparison method has a side effect, which can modify the _hasDuplicates field.
            // This can either be true or false at any given time. 
            if (_hasDuplicates)
            {
                // leave just the latest
                for (int i = 0; i < properties.Count - 1; i++)
                {
                    if (properties[i].Property == properties[i + 1].Property)
                    {
                        _cachedSorts[index].FinalCount--;
                        _cachedSorts[index].Sorting[i + 1] = new PropertyPosition
                        {
                            Property = properties[i + 1].Property,
                            // set it to the previous value, so it'll just overwrite
                            // this saves us a check and more complex code
                            SortedPosition = i 
                        };

                        properties.RemoveAt(i + 1);

                        i--;
                    }
                }
            }
            

            for (int i = 0; i < _cachedSorts[index].Sorting.Count; i++)
            {
                var propPos = _cachedSorts[index].Sorting[i];
                propPos.SortedPosition = -1;
                for (int j = 0; j < properties.Count; j++)
                {
                    if (properties[j].Property == propPos.Property)
                    {
                        propPos.SortedPosition = j;
                        break;
                    }
                    
                }
            }

        }

        private void UpdatePropertiesSortOrder()
        {
            int index = 0;
            foreach (var o in _propertiesSortOrder)
            {
                o.Key.GlobalSortOrder = index++;
            }
            _propertiesNeedSorting = false;
        }

        private bool _hasDuplicates;

        int IComparer<BlittableJsonDocumentBuilder.PropertyTag>.Compare(BlittableJsonDocumentBuilder.PropertyTag x, BlittableJsonDocumentBuilder.PropertyTag y)
        {
            var compare = x.Property.GlobalSortOrder - y.Property.GlobalSortOrder;
            if (compare == 0)
            {
                _hasDuplicates = true;
                return y.Position - x.Position;
            }
            return compare;
        }

        public LazyStringValue GetProperty(int index)
        {
            return _docPropNames[index].Comparer;
        }

        public void NewDocument()
        {
            PropertiesDiscovered = 0;
        }
    }
}