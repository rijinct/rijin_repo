from loggerUtil import loggerUtil
import test1
logFile='testLog'
LOGGER = loggerUtil.__call__().get_logger(logFile)
LOGGER.error("test")
test1.test1()
print(loggerUtil.__call__())
