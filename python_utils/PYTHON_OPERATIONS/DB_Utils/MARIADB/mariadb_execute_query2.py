from MARIADB.mariadb_connection_execute import MariaDBConnExec
from WORKFLOW_CONTEXT.workflow_context import WorkflowContext

class mariadbExecute(MariaDBConnExec):
    
    def __init__(self,context):
        super().execute(context)
    
    #This gets overridden. No need to put @override like java
    def execute_TLCquery(self):
        print('hi')


if __name__ == "__main__":
    context = WorkflowContext()
    context.setProperty( 'TLC', 'True')
    mariadbExecute(context)
    #obj.execute()