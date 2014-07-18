#!/usr/bin/env python

from subprocess import Popen,PIPE
import re
import sys

replc = sys.argv[1]
bindDN = sys.argv[2]
bindPW = sys.argv[3]


m = re.match(r".*,ibm-replicaGroup=default,(.*)", replc)
ReplContextD = {'ReplContext':m.group(1)}

def group_iter(iterator, spliter="\n"):
    accmulator = []
    for item in iterator:
        if item == spliter :
            yield tuple(accmulator)
            accmulator = []
        else:
            accmulator.append(item)

def convert(repltopo):
    for line in repltopo:
        m1 = re.match(r"cn=(?P<Consumer>.*),cn=(?P<Supplier>.*),ibm-replicaGroup=.*", line)
        m2 = re.match(r"^.*PendingChangeCount=(?P<PendingChangeCount>\d+).*", line)
        m3 = re.match(r"^.*State=(?P<QueueStatus>\w+).*", line)
        if m1:
            ret1 = m1.groupdict()
        elif m2:
            ret2 = m2.groupdict()
        elif m3:
            ret3 = m3.groupdict()
    return dict(ret1.items() + ret2.items() + ret3.items() + ReplContextD.items())

def output(d, separator="|"):
    sequence = ('ReplContext', 'Supplier', 'Consumer', 'PendingChangeCount', 'QueueStatus')
    fline = []
    for i in sequence:
        fline.append(d[i])
    print separator.join(fline)


ret = Popen(['idsldapsearch', '-D', bindDN, '-w', bindPW, '-b', replc, 'objectclass=ibm-replicationAgreement', 'ibm-replicationPendingChangeCount', 'ibm-replicationState', 'ibm-replicaURL'], stdout=PIPE, stderr=PIPE)
out = ret.stdout.readlines()
out.append("\n")  ### in case the output include singel entry

for repl in group_iter(out):
    output(convert(repl))
