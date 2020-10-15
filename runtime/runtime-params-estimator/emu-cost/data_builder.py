import re
import sys

re1 = re.compile('Using (\d+) accounts')
re2 = re.compile('executed (\d+) instructions; (\d+) bytes read; (\d+) bytes written')

accounts = 0
interesting = [ 1000000 ]

for line in sys.stdin:
    m = re1.match(line)
    if m:
        accounts = int(m.group(1))
    m = re2.match(line)
    if m and accounts in interesting:
        insn = m.group(1)
        read = m.group(2)
        written = m.group(3)
        print insn, "r"+str(accounts), read
        print insn, "w"+str(accounts), written
