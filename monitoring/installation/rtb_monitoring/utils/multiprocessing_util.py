import multiprocessing
import time


def multiprocess(function):
    processes = []
    TIMEOUT = 1800

    def replacement_function(args, output_results=None):
        global target_time
        if output_results is not None:
            for value in args:
                launch_process(value, output_results)
        else:
            for value in args:
                launch_process(value)

        target_time = time.time() + TIMEOUT
        while not done():
            time.sleep(1)
        kill_processes()
        join_processes()

    def launch_process(value, output_results=None):
        if output_results is not None:
            process = multiprocessing.Process(target=function,
                                              args=(value, output_results))
        else:
            process = multiprocessing.Process(target=function, args=(value, ))
        processes.append(process)
        process.start()

    def done():
        global target_time
        if time.time() >= target_time:
            return True
        for process in processes:
            if process.is_alive():
                return False
        return True

    def kill_processes():
        for process in processes:
            if process.is_alive():
                process.terminate()

    def join_processes():
        for process in processes:
            process.join()

    return replacement_function
