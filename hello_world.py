"""
A Luigi task is where the execution of your pipeline and the definition of each task’s input and output
dependencies take place. Tasks are the building blocks that you will create your pipeline from. You define them in a
class, which contains:

A run() method that holds the logic for executing the task.An output() method that returns the artifacts generated
by the task. The run() method populates these artifacts. An optional input() method that returns any additional tasks
in your pipeline that are required to execute the current task. The run() method uses these to carry out the task.
"""
import luigi

"""
HelloLuigi() is a Luigi task by adding the luigi.Task mixin (Class contains methods for use by other classes 
without having to be the parent class.
The output() method defines one or more Target outputs that your task produces. Luigi.LocalTarget is a local file.
 """
class HelloLuigi(luigi.Task):

    def output(self):
        return luigi.LocalTarget('hello-luigi.txt')

    def run(self):
        with self.output().open("w") as outfile:
            outfile.write("Hello Luigi!")
