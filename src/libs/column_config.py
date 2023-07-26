"""
Column's type for value conversions.
Here, add column names in their respective
sets, these columns are independent of 
which tables or schema they belong to.
"""
# SELECT TO_TIMESTAMP('varchar/string')
TIME_BASED = set()
# SELECT INET_ATON(HEX_TO_INTEGER(TO_HEX('varchar/string'))) FOR IPv4 32bit address;
IPV4_BASED = set()
# SELECT V6_NTOA('varbinary'))) FOR IPv6 64bit address from varbinary to varchar
# SELECT V6_ATON('varchar/string'))) FOR IPv6 64bit address from varchar to varbinary
IPV6_BASED = set()
NUMERICAL_BASED = set()

__all__ = [TIME_BASED, IPV4_BASED, IPV6_BASED, NUMERICAL_BASED]
