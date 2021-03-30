import threading
import time

def thread_function(name):
    print("Thread {} starting".format(name))
    time.sleep(2)
    print("Thread {} finishing". format(name))


if __name__ == "__main__":

    threads = list()
    for i in range(3):
        x = threading.Thread(target=thread_function,args=(i,))
        threads.append(x)
        x.start()

    print('Run')

    for index, thread in enumerate(threads):
        print('Before joining thread {}'. format(index))
        thread.join()
        print('done with thread {}'. format(index))

