# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import binascii
import sys
import re
import calendar
import math
import wcwidth
from collections import defaultdict
from displaying import colorme, get_str, FormattedValue, DEFAULT_VALUE_COLORS, NO_COLOR_MAP
from cassandra.cqltypes import EMPTY
from cassandra.util import datetime_from_timestamp
from util import UTC

unicode_controlchars_re = re.compile(r'[\x00-\x31\x7f-\xa0]')
controlchars_re = re.compile(r'[\x00-\x31\x7f-\xff]')


def _show_control_chars(match):
    txt = repr(match.group(0))
    if txt.startswith('u'):
        txt = txt[2:-1]
    else:
        txt = txt[1:-1]
    return txt

bits_to_turn_red_re = re.compile(r'\\([^uUx]|u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|U[0-9a-fA-F]{8})')


def _make_turn_bits_red_f(color1, color2):
    def _turn_bits_red(match):
        txt = match.group(0)
        if txt == '\\\\':
            return '\\'
        return color1 + txt + color2
    return _turn_bits_red

default_null_placeholder = 'null'
default_time_format = ''
default_float_precision = 3
default_colormap = DEFAULT_VALUE_COLORS
empty_colormap = defaultdict(lambda: '')


def format_by_type(val, cqltype, encoding, colormap=None, addcolor=False,
                   nullval=None, time_format=None, float_precision=None,
                   decimal_sep=None, thousands_sep=None, boolean_styles=None):
    if nullval is None:
        nullval = default_null_placeholder
    if val is None:
        return colorme(nullval, colormap, 'error')
    if addcolor is False:
        colormap = empty_colormap
    elif colormap is None:
        colormap = default_colormap
    if time_format is None:
        time_format = default_time_format
    if float_precision is None:
        float_precision = default_float_precision
    return format_value(val, cqltype=cqltype, encoding=encoding, colormap=colormap,
                        time_format=time_format, float_precision=float_precision,
                        nullval=nullval, decimal_sep=decimal_sep, thousands_sep=thousands_sep,
                        boolean_styles=boolean_styles)


def color_text(bval, colormap, displaywidth=None):
    # note that here, we render natural backslashes as just backslashes,
    # in the same color as surrounding text, when using color. When not
    # using color, we need to double up the backslashes so it's not
    # ambiguous. This introduces the unique difficulty of having different
    # display widths for the colored and non-colored versions. To avoid
    # adding the smarts to handle that in to FormattedValue, we just
    # make an explicit check to see if a null colormap is being used or
    # not.
    if displaywidth is None:
        displaywidth = len(bval)
    tbr = _make_turn_bits_red_f(colormap['blob'], colormap['text'])
    coloredval = colormap['text'] + bits_to_turn_red_re.sub(tbr, bval) + colormap['reset']
    if colormap['text']:
        displaywidth -= bval.count(r'\\')
    return FormattedValue(bval, coloredval, displaywidth)


def format_value_default(val, colormap, **_):
    val = str(val)
    escapedval = val.replace('\\', '\\\\')
    bval = controlchars_re.sub(_show_control_chars, escapedval)
    return bval if colormap is NO_COLOR_MAP else color_text(bval, colormap)

# Mapping cql type base names ("int", "map", etc) to formatter functions,
# making format_value a generic function
_formatters = {}


def format_value(val, cqltype=None, **kwargs):
    if val == EMPTY:
        return format_value_default('', **kwargs)

    formatter = get_formatter(val, cqltype)
    return formatter(val, cqltype=cqltype, **kwargs)


def get_formatter(val, cqltype):
    formatter_type = cqltype if cqltype is not None and cqltype in _formatters else type(val).__name__
    return _formatters.get(formatter_type, format_value_default)


def formatter_for(typname):
    def registrator(f):
        _formatters[typname] = f
        return f
    return registrator


@formatter_for('bytearray')
def format_value_blob(val, colormap, **_):
    bval = '0x' + binascii.hexlify(str(val))
    return colorme(bval, colormap, 'blob')
formatter_for('buffer')(format_value_blob)
formatter_for('blob')(format_value_blob)


def format_python_formatted_type(val, colormap, color, quote=False):
    bval = str(val)
    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, color)


@formatter_for('Decimal')
def format_value_decimal(val, float_precision, colormap, decimal_sep=None, thousands_sep=None, **_):
    if (decimal_sep and decimal_sep != '.') or thousands_sep:
        return format_floating_point_type(val, colormap, float_precision, decimal_sep, thousands_sep)
    return format_python_formatted_type(val, colormap, 'decimal')


@formatter_for('UUID')
def format_value_uuid(val, colormap, **_):
    return format_python_formatted_type(val, colormap, 'uuid')


@formatter_for('inet')
def formatter_value_inet(val, colormap, quote=False, **_):
    return format_python_formatted_type(val, colormap, 'inet', quote=quote)


@formatter_for('bool')
def format_value_boolean(val, colormap, boolean_styles=None, **_):
    if boolean_styles:
        val = boolean_styles[0] if val else boolean_styles[1]
    return format_python_formatted_type(val, colormap, 'boolean')


def format_floating_point_type(val, colormap, float_precision, decimal_sep=None, thousands_sep=None, **_):
    if math.isnan(val):
        bval = 'NaN'
    elif math.isinf(val):
        bval = 'Infinity' if val > 0 else '-Infinity'
    else:
        if thousands_sep:
            dpart, ipart = math.modf(val)
            bval = format_integer_with_thousands_sep(ipart, thousands_sep)
            dpart_str = ('%.*f' % (float_precision, math.fabs(dpart)))[2:].rstrip('0')
            if dpart_str:
                bval += '%s%s' % ('.' if not decimal_sep else decimal_sep, dpart_str)
        else:
            exponent = int(math.log10(abs(val))) if abs(val) > sys.float_info.epsilon else -sys.maxint - 1
            if -4 <= exponent < float_precision:
                # when this is true %g will not use scientific notation,
                # increasing precision should not change this decision
                # so we increase the precision to take into account the
                # digits to the left of the decimal point
                float_precision = float_precision + exponent + 1
            bval = '%.*g' % (float_precision, val)
            if decimal_sep:
                bval = bval.replace('.', decimal_sep)

    return colorme(bval, colormap, 'float')

formatter_for('float')(format_floating_point_type)


def format_integer_type(val, colormap, thousands_sep=None, **_):
    # base-10 only for now; support others?
    bval = format_integer_with_thousands_sep(val, thousands_sep) if thousands_sep else str(val)
    return colorme(bval, colormap, 'int')

# We can get rid of this in cassandra-2.2
if sys.version_info >= (2, 7):
    def format_integer_with_thousands_sep(val, thousands_sep=','):
        return "{:,.0f}".format(val).replace(',', thousands_sep)
else:
    def format_integer_with_thousands_sep(val, thousands_sep=','):
        if val < 0:
            return '-' + format_integer_with_thousands_sep(-val, thousands_sep)
        result = ''
        while val >= 1000:
            val, r = divmod(val, 1000)
            result = "%s%03d%s" % (thousands_sep, r, result)
        return "%d%s" % (val, result)

formatter_for('long')(format_integer_type)
formatter_for('int')(format_integer_type)


@formatter_for('date')
def format_value_timestamp(val, colormap, time_format, quote=False, **_):
    bval = strftime(time_format, calendar.timegm(val.utctimetuple()))
    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, 'timestamp')

formatter_for('datetime')(format_value_timestamp)


def strftime(time_format, seconds):
    tzless_dt = datetime_from_timestamp(seconds)
    return tzless_dt.replace(tzinfo=UTC()).strftime(time_format)


@formatter_for('str')
def format_value_text(val, encoding, colormap, quote=False, **_):
    escapedval = val.replace(u'\\', u'\\\\')
    if quote:
        escapedval = escapedval.replace("'", "''")
    escapedval = unicode_controlchars_re.sub(_show_control_chars, escapedval)
    bval = escapedval.encode(encoding, 'backslashreplace')
    if quote:
        bval = "'%s'" % bval

    return bval if colormap is NO_COLOR_MAP else color_text(bval, colormap, wcwidth.wcswidth(bval.decode(encoding)))

# name alias
formatter_for('unicode')(format_value_text)


def get_sub_types(val, num_sub_types):
    """
    Return the cql sub-types between brackets but ignoring frozen types.
    """
    if val is None or '<' not in val:
        return [None] * num_sub_types

    val = val[val.find('<') + 1:-1]
    if val.startswith('frozen'):
        val = val[val.find('<') + 1:-1]

    last = 0
    level = 0
    quote = False
    ret = []
    for i, c in enumerate(val):
        if c == '<':
            level += 1
        elif c == '>':
            level -= 1
        elif c == '\'':
            quote = not quote
        elif c == ',' and level == 0 and not quote:
            ret.append(val[last:i].strip())
            last = i + 1
    else:
        if last < len(val) - 1:
            ret.append(val[last:].strip())

    if len(ret) < num_sub_types:
        # for list and set all types are identical to the first one
        item = ret[-1] if ret else None
        ret.extend([item] * (num_sub_types - len(ret)))

    return ret


def format_simple_collection(val, cqltype, lbracket, rbracket, encoding,
                             colormap, time_format, float_precision, nullval,
                             decimal_sep, thousands_sep, boolean_styles):
    stypes = get_sub_types(cqltype, len(val))
    subs = [format_value(sval, cqltype=stype, encoding=encoding, colormap=colormap,
                         time_format=time_format, float_precision=float_precision,
                         nullval=nullval, quote=True, decimal_sep=decimal_sep,
                         thousands_sep=thousands_sep, boolean_styles=boolean_styles)
            for sval, stype in zip(val, stypes)]
    bval = lbracket + ', '.join(get_str(sval) for sval in subs) + rbracket
    if colormap is NO_COLOR_MAP:
        return bval

    lb, sep, rb = [colormap['collection'] + s + colormap['reset']
                   for s in (lbracket, ', ', rbracket)]
    coloredval = lb + sep.join(sval.coloredval for sval in subs) + rb
    displaywidth = 2 * len(subs) + sum(sval.displaywidth for sval in subs)
    return FormattedValue(bval, coloredval, displaywidth)


@formatter_for('list')
def format_value_list(val, cqltype, encoding, colormap, time_format, float_precision, nullval,
                      decimal_sep, thousands_sep, boolean_styles, **_):
    return format_simple_collection(val, cqltype, '[', ']', encoding, colormap,
                                    time_format, float_precision, nullval,
                                    decimal_sep, thousands_sep, boolean_styles)


@formatter_for('tuple')
def format_value_tuple(val, cqltype, encoding, colormap, time_format, float_precision, nullval,
                       decimal_sep, thousands_sep, boolean_styles, **_):
    return format_simple_collection(val, cqltype, '(', ')', encoding, colormap,
                                    time_format, float_precision, nullval,
                                    decimal_sep, thousands_sep, boolean_styles)


@formatter_for('set')
def format_value_set(val, cqltype, encoding, colormap, time_format, float_precision, nullval,
                     decimal_sep, thousands_sep, boolean_styles, **_):
    return format_simple_collection(sorted(val), cqltype, '{', '}', encoding, colormap,
                                    time_format, float_precision, nullval,
                                    decimal_sep, thousands_sep, boolean_styles)
formatter_for('frozenset')(format_value_set)
# This code is used by cqlsh (bundled driver version 2.7.2 using sortedset),
# and the dtests, which use whichever driver on the machine, i.e. 3.0.0 (SortedSet)
formatter_for('SortedSet')(format_value_set)
formatter_for('sortedset')(format_value_set)


@formatter_for('dict')
def format_value_map(val, cqltype, encoding, colormap, time_format, float_precision, nullval,
                     decimal_sep, thousands_sep, boolean_styles, **_):
    def subformat(v, t):
        return format_value(v, cqltype=t, encoding=encoding, colormap=colormap,
                            time_format=time_format, float_precision=float_precision,
                            nullval=nullval, quote=True, decimal_sep=decimal_sep,
                            thousands_sep=thousands_sep, boolean_styles=boolean_styles)

    sub_types = get_sub_types(cqltype, 2)
    subs = [(subformat(k, sub_types[0]), subformat(v, sub_types[1])) for (k, v) in sorted(val.items())]
    bval = '{' + ', '.join(get_str(k) + ': ' + get_str(v) for (k, v) in subs) + '}'
    if colormap is NO_COLOR_MAP:
        return bval

    lb, comma, colon, rb = [colormap['collection'] + s + colormap['reset']
                            for s in ('{', ', ', ': ', '}')]
    coloredval = lb \
        + comma.join(k.coloredval + colon + v.coloredval for (k, v) in subs) \
        + rb
    displaywidth = 4 * len(subs) + sum(k.displaywidth + v.displaywidth for (k, v) in subs)
    return FormattedValue(bval, coloredval, displaywidth)
formatter_for('OrderedDict')(format_value_map)
formatter_for('OrderedMap')(format_value_map)
formatter_for('OrderedMapSerializedKey')(format_value_map)


def format_value_utype(val, cqltype, encoding, colormap, time_format, float_precision, nullval,
                       decimal_sep, thousands_sep, boolean_styles, **_):
    def format_field_value(v, t):
        if v is None:
            return colorme(nullval, colormap, 'error')
        return format_value(v, cqltype=t, encoding=encoding, colormap=colormap,
                            time_format=time_format, float_precision=float_precision,
                            nullval=nullval, quote=True, decimal_sep=decimal_sep,
                            thousands_sep=thousands_sep, boolean_styles=boolean_styles)

    def format_field_name(name):
        return format_value_text(name, encoding=encoding, colormap=colormap, quote=False)

    stypes = get_sub_types(cqltype, len(val))
    subs = [(format_field_name(k), format_field_value(v, t)) for ((k, v), t) in zip(val._asdict().items(), stypes)]
    bval = '{' + ', '.join(get_str(k) + ': ' + get_str(v) for (k, v) in subs) + '}'
    if colormap is NO_COLOR_MAP:
        return bval

    lb, comma, colon, rb = [colormap['collection'] + s + colormap['reset']
                            for s in ('{', ', ', ': ', '}')]
    coloredval = lb \
        + comma.join(k.coloredval + colon + v.coloredval for (k, v) in subs) \
        + rb
    displaywidth = 4 * len(subs) + sum(k.displaywidth + v.displaywidth for (k, v) in subs)
    return FormattedValue(bval, coloredval, displaywidth)
