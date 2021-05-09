import multiprocessing
import time


def multiprocess(timeout=10):
    processes = []

    def replacement_function(function):

        def wrap_process(*args):

            def launch_process(value):
                process = multiprocessing.Process(target=function,
                                                  args=(value,))
                processes.append(process)
                process.start()

            def done():
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

            for value in args[0]:
                launch_process(value)
            target_time = time.time() + timeout
            while not done():
                time.sleep(1)
            kill_processes()
            join_processes()

        return wrap_process

    return replacement_function
