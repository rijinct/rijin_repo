import sched, time
s = sched.scheduler(time.time, time.sleep)
def do_something(sc): 
    print('Doing stuff...')
    # do your stuff
    s.enter(4, 1, do_something, (sc,))

s.enter(1, 1, do_something, (s,))
s.run()