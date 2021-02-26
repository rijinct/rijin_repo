import configparser, os

class PropertyFileUtil:

        def __init__(self, monitoringType=None, propertyFileSection=None):
                if monitoringType and "allitems" not in monitoringType:
                    self.monitoringType = monitoringType
                self.propertyFileSection = propertyFileSection
                self.configLoad = configparser.RawConfigParser()
                self.configLoad.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'monitoring.properties'))

        def getValueForKey(self):
                return self.configLoad.get(self.propertyFileSection,self.monitoringType)

        def getAllItemsInSection(self):
                return dict(self.configLoad.items(self.propertyFileSection))

        def getAllDirectories(self):
               outputDirectories = []
               for section in self.configLoad.sections():
                       if section == "DirectorySection":
                               for (key,value) in self.configLoad.items(section):
                                       outputDirectories.append(value)
               return outputDirectories

        