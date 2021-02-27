import luigi


class WritePipelineTask(luigi.Task):

    def output(self):
        return luigi.LocalTarget(r"C:\Users\rithomas\Desktop\NokiaCemod\14. "
                                 r"MyProjects\rijin_git\rijin_repo\luigi\conf/output_one.txt")

    def run(self):
        with self.output().open("w") as output_file:
            output_file.write("pipeline")


class AddMyTask(luigi.Task):

    def output(self):
        return luigi.LocalTarget(r"C:\Users\rithomas\Desktop\NokiaCemod\14. "
                                 r"MyProjects\rijin_git\rijin_repo\luigi\conf/output_two.txt")

    def requires(self):
        return WritePipelineTask()

    def run(self):
        with self.input().open("r") as input_file:
            line = input_file.read()

        with self.output().open("w") as output_file:
            decorated_line = "My "+line
            output_file.write(decorated_line)

if __name__ == '__main__':
    luigi.run()