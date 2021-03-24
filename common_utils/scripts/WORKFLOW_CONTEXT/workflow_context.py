from abc import ABC,abstractmethod

class WorkflowContext:
    
    def __init__(self):
        self.properties = {}
        
    def setProperty(self,key,value):
        self.properties[key] = value
    
    def getProperty(self,key):
        return self.properties[key]
    
    
    
    
    
if __name__ == "__main__":
    context = WorkflowContext()
    context.setProperty('alt', True)
    print(context.getProperty('alt'))