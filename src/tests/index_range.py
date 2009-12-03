#!/usr/bin/env python

import sys
import re

class Output:
    def __init__(self):
        self.indentlevel = 0
        self.buffer = []

    def blank(self):
        self.buffer.append('')

    def line(self, s):
        self.buffer.append('%s%s' % ('    ' * self.indentlevel, s))

    def raw(self, s):
        self.buffer.append(s)

    def contents(self):
        return '\n'.join(self.buffer + [''])

class MultiDict:
    def __init__(self, s):
        self.buckets = {}
        self.nitems = 0

        s = s.strip()
        if s:
            for item in s.split(','):
                key, value = item.split(':')
                key = key.strip()
                value = value.strip()
                if key in self.buckets:
                    self.buckets[key].append(value)
                else:
                    self.buckets[key] = [value]
                self.nitems += 1

    def __len__(self):
        return self.nitems

    def items(self):
        r = []
        for key, bucket in self.buckets.items():
            for value in bucket:
                r.append((key, value))
        return r

def range_query_assert(line):
    m = re.search('^\s*RANGE_QUERY_ASSERT\s*\(\s*"\s*(\[|\()\s*(-?\d+)\s*,\s*(-?\d+)(\]|\))\s*=>\s*{(.*)}\s*"\s*\)\s*;\s*$', line)
    assert m is not None, line

    left = m.group(1)
    start = int(m.group(2))
    stop = int(m.group(3))
    right = m.group(4)
    result_set = MultiDict(m.group(5))
    buf_size = len(result_set) + 2

    # buffers
    out.line('int    keybuf[%d];' % (buf_size))
    out.line('double valbuf[%d];' % (buf_size))
    out.line('memset(keybuf, 0xAB, sizeof(keybuf));')
    out.line('memset(valbuf, 0xCD, sizeof(valbuf));')

    # execute RangeQuery, make sure it returned the right number of pairs
    out.line('CPPUNIT_ASSERT(index->RangeQuery(%d, %s, %d, %s, %d, keybuf + 1, valbuf + 1) == %d);' % (
           start, 'true' if left  == '[' else 'false',
           stop,  'true' if right == ']' else 'false',
           len(result_set) + 1,
           len(result_set)))

    # make sure the result didn't clobber the first or last element
    out.line('CPPUNIT_ASSERT(keybuf[0]  == static_cast<int>(0xABABABAB));')
    out.line('CPPUNIT_ASSERT(keybuf[%d] == static_cast<int>(0xABABABAB));' % (len(result_set) + 1))
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0])  == 0xCDCDCDCDCDCDCDCD);')
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[%d]) == 0xCDCDCDCDCDCDCDCD);' % (len(result_set) + 1))

    # make sure the buffers contain all the pairs
    out.line('std::multimap<int, double> expected;')
    out.line('std::multimap<int, double> actual;')
    for (k,v) in result_set.items():
        out.line('expected.insert(std::pair<int, double>(%s, %s));' % (k,v))
    out.line('for (int i = 1; i <= %d; i++) {' % (len(result_set)))
    out.line('    actual.insert(std::pair<int, double>(keybuf[i], valbuf[i]));')
    out.line('}')
    out.line('CPPUNIT_ASSERT(multimaps_equal(&expected, &actual));')

def multi_lookup_assert(line):
    m = re.search('^\s*MULTI_LOOKUP_ASSERT\s*\(\s*"\s*(-?\d+)\s*=>\s*{(.*)}\s*"\s*\)\s*;\s*$', line)
    assert m is not None, line

    key = int(m.group(1))
    result_set = eval('set([%s])' % m.group(2))
    buf_size = len(result_set) + 2

    # buffers
    out.line('double valbuf[%d];' % (buf_size))
    out.line('memset(valbuf, 0xCD, sizeof(valbuf));')

    # execute Lookup, make sure it returned the right number of values
    out.line('CPPUNIT_ASSERT(index->Lookup(%d, %d, valbuf + 1) == %d);' % (
           key,
           len(result_set) + 1,
           len(result_set)))

    # make sure the result didn't clobber the first or last element
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0])  == 0xCDCDCDCDCDCDCDCD);')
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[%d]) == 0xCDCDCDCDCDCDCDCD);' % (len(result_set) + 1))

    # make sure the buffers contain all the pairs
    out.line('std::set<double> expected;')
    out.line('std::set<double> actual;')
    for v in result_set:
        out.line('expected.insert(%s);' % v)
    out.line('for (int i = 1; i <= %d; i++) {' % (len(result_set)))
    out.line('    actual.insert(valbuf[i]);')
    out.line('}')
    out.line('CPPUNIT_ASSERT(sets_equal(&expected, &actual));')

out = Output()
out.indentlevel += 1

lines = sys.stdin.readlines()
i = 0
prev = ''
for line in lines:
    i += 1
    if len(line) > 1 and line[-2] == '\\':
        prev += line[:-2].rstrip()
        continue
    else:
        if prev:
            line = prev + line.lstrip()
            prev = ''
    if 'RANGE_QUERY_ASSERT' in line:
        handler = range_query_assert
    elif 'MULTI_LOOKUP_ASSERT' in line:
        handler = multi_lookup_assert
    else:
        out.raw(line[:-1])
        continue

    line = line.replace('""', '')

    out.blank()
    out.line('{ // %s' % line.strip())
    out.indentlevel += 1

    handler(line)

    out.indentlevel -= 1
    out.line('}')


open(sys.argv[1], 'w').write(out.contents())
