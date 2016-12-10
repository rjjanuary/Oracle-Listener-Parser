from listener_parser import listener_parser

lp = listener_parser('/tmp/listener.small')
for x in lp.getrecords():
    print x